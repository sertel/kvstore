{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ImplicitParams #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables, NamedFieldPuns, OverloadedStrings,
  FlexibleContexts, PartialTypeSignatures, RecordWildCards, TypeApplications #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE DeriveAnyClass    #-}

import Control.Lens
import Control.DeepSeq
import Control.Monad
import Control.Monad.State
import Control.Arrow (first, second, (***), (&&&))
import Control.Concurrent (threadDelay)
import System.Random
import Data.Void
import Control.DeepSeq
import Control.Exception
import System.Exit
import System.IO
import System.Mem
import System.CPUTime

import GHC.Conc.Sync (setNumCapabilities,numCapabilities)

import Data.Aeson as AE
import Data.Aeson.TH as AE
import qualified Data.ByteString.Lazy as BS
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as Set
import qualified Data.Map as Map
import qualified Data.Text.Lazy as T
import Control.Monad.Random.Strict
import Data.IORef
import Data.List
import Data.Time.Clock.POSIX
import qualified Data.Vector as V
import Kvservice_Types
import Kvstore.KVSTypes
import Kvstore.Serialization
import Kvstore.InputOutput (store)
import Data.Dynamic2

import qualified Monad.StreamsBasedExplicitAPI as SBFM
import Control.Monad.Stream (MonadStream)
import Control.Monad.Stream.Chan
import Control.Monad.Stream.PinnedChan
import Control.Monad.Stream.Par
import qualified Monad.StreamsBasedFreeMonad as SBFM
import qualified Monad.FuturesBasedMonad as FBM
import Kvstore.Ohua.FBM.KeyValueService (pureUnitSf)

import qualified Kvstore.Ohua.SBFM.KeyValueService as KVS
import qualified Kvstore.Ohua.SBFM.RequestHandling as SBFM
import qualified Kvstore.Ohua.FBM.KeyValueService as FBM
import qualified Kvstore.Ohua.FBM.RequestHandling as FBM
import qualified Data.StateElement as FBM

import Kvstore.Ohua.RequestHandling (storeTable)

import Requests
import ServiceConfig
import Versions
import MBConfig

import Statistics.Sample (mean)

import Debug.Trace
import Text.Printf

import GHC.Generics


forceA :: (NFData a, Applicative f) => a -> f a
forceA a = a `deepseq` pure a

forceA_ :: (NFData a, Applicative f) => a -> f ()
forceA_ a = a `deepseq` pure ()

instance RandomGen Void where
  next g = (absurd g, g)
  split g = (g, g)

initState :: Bool -> IO (KVSState MockDB)
initState useEncryption = do
    db <- newIORef HM.empty
    (enc, dec) <-
        if useEncryption
            then aesEncryption
            else pure (noEnc, noDec)
    return $
        KVSState
            HM.empty
            (makeNoWait db) -- latency is 0 for loading the DB and then
                          -- we set it for the requests
            binarySerialization
            binaryDeserialization
            zlibComp
            zlibDecomp
            enc
            dec

valueTemplate v = "value-" ++ show v

fieldTemplate f = "field-" ++ show f

keyTemplate k = "key-" ++ show k

tableTemplate t = "table-" ++ show t

data RangeGen =
    RangeGen Int
             Int
             StdGen

instance RandomGen RangeGen where
    next (RangeGen lo hi g) =
        let (i, g') = randomR (lo, hi) g
         in (i, RangeGen lo hi g')
    split (RangeGen lo hi g) =
        let (g1, g2) = split g
         in (RangeGen lo hi g1, RangeGen lo hi g2)

newtype LinearGen =
    LinearGen Int

instance RandomGen LinearGen where
    next (LinearGen i) = (i, LinearGen $ i + 1)
    split (LinearGen i) = (LinearGen i, LinearGen i)

data BenchmarkState g = BenchmarkState
    { _fieldCount :: Int
    , _fieldSelection :: RangeGen
    , _valueSizeGen :: RangeGen
    , _tableCount :: Int
    , _tableSelection :: RangeGen
    , _keySelection :: g
    , _operationSelection :: RangeGen
    , _fieldCountSelection :: RangeGen
    , _scanCountSelection :: RangeGen
    }

makeLenses ''BenchmarkState

-- lens does not work with RankNTypes :(
createValue ::
       forall g. RandomGen g
    => StateT (BenchmarkState g) IO String
createValue = do
    bmState <- get
    let valSizeGenerator = view valueSizeGen bmState
        (size, valSizeGenerator') = next valSizeGenerator
    put $ over valueSizeGen (const valSizeGenerator') bmState
    (return . concatMap valueTemplate . take size) [1,2 ..]

createRequest ::
       forall g. RandomGen g
    => StateT (BenchmarkState g) IO KVRequest
createRequest = do
    bmState <- get
    let opSelector = view operationSelection bmState
        (op, opSelector') = next opSelector
        bmState' = over operationSelection (const opSelector') bmState
        tableSelector = view tableSelection bmState
        (tableId, tableSelector') = next tableSelector
        table = tableTemplate tableId
        bmState'' = over tableSelection (const tableSelector') bmState'
        keySelector = view keySelection bmState
        (keyId, keySelector') = next keySelector
        key = keyTemplate keyId
        bmState''' = over keySelection (const keySelector') bmState''
    put bmState'''
    case op of
        0 -> prepareINSERT table key <$> createINSERTEntry
        1 -> prepareUPDATE table key <$> createUPDATEEntry
        2 -> prepareREAD table key . Set.map fieldTemplate <$> getFields
        3 ->
            prepareSCAN table key <$> getScanCount <*>
            (Set.map fieldTemplate <$> getFields)
        4 -> return $ prepareDELETE table key
        _ -> error $ "No such operation: " ++ show op
  where
    getFieldsAndValues ::
           forall g. RandomGen g
        => [Int]
        -> StateT (BenchmarkState g) IO (HM.HashMap String String)
    getFieldsAndValues =
        fmap HM.fromList .
        mapM (\i -> (,) <$> pure (fieldTemplate i) <*> createValue)
    createINSERTEntry ::
           forall g. RandomGen g
        => StateT (BenchmarkState g) IO (HM.HashMap String String)
    createINSERTEntry =
        getFieldsAndValues . flip take [1,2 ..] . view fieldCount =<< get
    getFields ::
           forall g. RandomGen g
        => StateT (BenchmarkState g) IO (Set.HashSet Int)
    getFields = do
        s <- get
        let fieldCountGen = view fieldCountSelection s
            (fieldCount, fieldCountGen') = next fieldCountGen
            s' = over fieldCountSelection (const fieldCountGen') s
            fieldSel = view fieldSelection s'
            (fieldSel', fields) =
                foldl
                    (\(sel, l) _ ->
                         let (f, sel') = next sel
                          in (sel', l ++ [f]))
                    (fieldSel, []) $
                take fieldCount [1,2 ..]
            s'' = over fieldSelection (const fieldSel') s'
        put s''
        return $ Set.fromList fields
    createUPDATEEntry ::
           forall g. RandomGen g
        => StateT (BenchmarkState g) IO (HM.HashMap String String)
    createUPDATEEntry = getFieldsAndValues . Set.toList =<< getFields
    getScanCount = do
        s <- get
        let scanCountSel = view scanCountSelection s
            (scanCount, scanCountSel') = next scanCountSel
        put $ over scanCountSelection (const scanCountSel') s
        return scanCount

workload ::
       forall g. RandomGen g
    => Int
    -> StateT (BenchmarkState g) IO (V.Vector KVRequest)
workload operationCount =
    V.fromList <$> mapM (const createRequest) [1 .. operationCount]

showState :: KVSState MockDB -> IO String
showState KVSState {_cache = cache, _storage = MockDB dbRef _ _} = do
    db <- readIORef dbRef
    return $ "Cache:\n" ++ show cache ++ "\nDB:\n" ++ show db

defaultBenchmarkState =
    BenchmarkState
        { _fieldCount = 10
        , _fieldSelection = RangeGen 0 10 $ mkStdGen 0
        , _valueSizeGen = RangeGen 5 10 $ mkStdGen 0
        , _tableCount = 20
        , _tableSelection = RangeGen 1 1 $ mkStdGen 0
        , _keySelection = LinearGen 1
        , _operationSelection = undefined
        , _fieldCountSelection = RangeGen 3 10 $ mkStdGen 0
        , _scanCountSelection = RangeGen 5 10 $ mkStdGen 0
        }
  -- traceM "recur

loadDB useEncryption tc keyCount = do
    s <- initState useEncryption
  -- fill the db first
    requests <-
        fmap V.concat $
        forM [0 .. tc - 1] $ \i ->
            evalStateT
                (workload keyCount)
                defaultBenchmarkState
                    { _operationSelection = RangeGen 0 0 $ mkStdGen 0 -- (INSERT only)
                    , _tableSelection = RangeGen i i $ mkStdGen 0
                    }
    -- tracem "requests (INSERT):"
    -- mapM (\i -> traceM $ show i ++ "\n" ) requests
    (responses, s'@KVSState {_storage = (MockDB db _ _)}) <-
        flip runStateT s $ ?execRequests requests
  -- adjust minLatency for benchmark requests
    content <- readIORef db
    forceA_ content
    newDB <- make db 20 1000000000000000000000000
    return s' {_storage = newDB}
  -- traceM "state after init:"
  -- traceM =<< showState s'
  -- traceM "done with insert."

reqBenchmarkState keyCount numTables fieldCount opSelect =
    defaultBenchmarkState
        { _keySelection = RangeGen 1 keyCount $ mkStdGen 0
        , _operationSelection =
              maybe (RangeGen 0 4) (\i -> RangeGen i i) opSelect $ mkStdGen 0
              -- (RangeGen 1 3 $ mkStdGen 0) -- _operationSelection (no INSERT, no DELETE)
        , _tableCount = numTables
        , _tableSelection = RangeGen 0 numTables $ mkStdGen 0
        , _fieldCount = fieldCount
        , _fieldCountSelection = RangeGen 0 fieldCount $ mkStdGen 0
        }

currentTimeMillis = round . (* 1000) <$> getPOSIXTime

-- then run some requests
runRequests ::
       (?execRequests :: ExecReqFn)
    => Int
    -> BenchmarkState RangeGen
    -> StateT (KVSState MockDB) IO Integer
runRequests operationCount bmState = do
    (requests, _) <- liftIO $ runStateT (workload operationCount) bmState
  -- traceM $ "requests:"
  -- mapM (\i -> traceM $ show i ++ "\n" ) requests
    forceA_ requests
    liftIO performGC
    start <- liftIO currentTimeMillis
    responses <- ?execRequests requests
    forceA_ responses
    --() <- join $ gets forceA_ -- forces the state
    stop <- liftIO currentTimeMillis
    let execTime = stop - start
  -- traceM "???????????????????????????????????????"
  -- traceM $ "responses:"
  -- mapM (\i -> traceM $ show i ++ "\n" a) responses
    unless (operationCount == length responses) $ liftIO $
      error "wrong number of responses"
    return execTime

defaultTableCount = 20


runMultipleBatches ::
       (?execRequests :: ExecReqFn) => BatchConfig -> IO Double
runMultipleBatches BatchConfig { useEncryption = useEncryption
                               , keyCount
                               , batchCount
                               , batchSize
                               , numTables
                               , numFields
                               , requestSelection
                               } = do
    s <- loadDB useEncryption numTables keyCount
    let bmState = reqBenchmarkState keyCount numTables numFields requestSelection
    execTimes <-
        flip evalStateT s $
        replicateM batchCount (runRequests batchSize bmState)
    let meanExecTime = mean $ V.fromList $ map fromIntegral execTimes
   -- traceM $ "mean execution time: " ++ show meanExecTime ++ " ms"
    return meanExecTime

confs =
  [ BatchConfig { keyCount = 100
                , batchCount = 30
                , batchSize = 30
                , useEncryption = True
                , numTables = 20
                , threadCount = c
                , systemVersion = v
                }
  | c <- [1..8]
  , v <- [Functional, Ohua_FBM, Ohua_SBFM]
  ]

outputFile = "100keys-20tables.json"

avg l = sum l `div` toInteger (length l)

testEachAction replications = do
    results <-
        sequence $ replicate replications runTest :: IO [Map.Map String (Map.Map _ Integer)]
    let stats =
            fmap
                (fmap avg)
                (Map.unionsWith (Map.unionWith mappend) $
                 map (fmap $ fmap pure) results :: Map.Map String (Map.Map _ [Integer]))
    writeFile
        "stat-results"
        (show $
         fmap (second (fmap (first show) . Map.toList)) $ Map.toList stats)
  where
    runTest = do
        statVar <- newIORef mempty
        let execWithCollectStats statname reqs = do
                modify (cache .~ mempty)
                liftIO performGC
                (res, stats) <- KVS.execRequestsFunctional0 reqs
                t0 <- liftIO getCPUTime
                forceA res
                t1 <- liftIO getCPUTime
                let stats' = Map.insert "setup/force-result" (t1 - t0) stats
                liftIO $
                    atomicModifyIORef statVar ((, ()) . ((statname, stats') :))
                pure res
        s <-
            let ?execRequests = KVS.execRequestsFunctional
             in loadDB True 10 100
        flip runStateT s $ do
            let ?execRequests = execWithCollectStats "insert"
             in insertEntry "table-0" "key-0" "field-0" "value-0"
            let ?execRequests = execWithCollectStats "read"
             in readEntry "table-0" "key-0" "field-0"
            let ?execRequests = execWithCollectStats "update"
             in updateEntry "table-0" "key-0" "field-0" "value-1"
            let ?execRequests = execWithCollectStats "delete"
             in deleteEntry "table-0" "key-0"
        Map.fromList <$> readIORef statVar


profileRequests :: IO ()
profileRequests = do
    s <-
        let ?execRequests = KVS.execRequestsFunctional
         in loadDB True numTables keyCount
    t0 <- currentTimeMillis
    stats <-
        flip evalStateT s $
        sequence $
        replicate 5 $ do
            (requests, _) <-
                liftIO $ runStateT (workload operationCount) bmState
            -- liftIO $ evaluate $ force requests
            forceA_ requests
            liftIO performGC
            (responses, stats) <- KVS.execRequestsFunctional0 requests
            t0 <- liftIO getCPUTime
            forceA_ responses
            t1 <- liftIO getCPUTime
            let execTime = t1 - t0
            pure $ Map.insert "setup/force-result" execTime stats
    t1 <- currentTimeMillis
    printf "Exec time: %d\n" (t1 - t0)
    writeFile "bench-stats" $
        show
            [ ( "benchmark"
              , map (first show) $
                Map.toList $
                fmap avg $
                Map.unionsWith mappend $ map (fmap (pure :: a -> [a])) stats)
            ]
  where
    operationCount = 30
    bmState = reqBenchmarkState keyCount numTables fieldCount Nothing
    numTables = 20
    keyCount = 25
    fieldCount = 5

type RawWritePipeline = [(T.Text, Table)] -> StateT (KVSState MockDB) IO ()

pureWritePipeline :: RawWritePipeline
pureWritePipeline = mapM_ $ uncurry store

sbfmWritePipeline :: MonadStream m => (m _ -> IO _) -> [(T.Text, Table)] -> StateT (KVSState MockDB) IO _
sbfmWritePipeline runner tables0 =
    get >>= \KVSState {..} ->
        fmap snd .
        -- void .
        liftIO $
        runner .
        flip
            -- SBFM.runAlgo
            SBFM.runAlgoWStats
            [ undefined
            , toDyn _serializer
            , toDyn _compression
            , toDyn _encryption
            , undefined
            ] =<<
        SBFM.createAlgo
            (do tables <- SBFM.sfConst tables0
                db <- SBFM.sfConst _storage
                let f tabAndKey = do
                        tab <-
                            SBFM.liftWithIndexNamed
                                noStateIdx
                                "aux/snd"
                                (pureUnitSf . snd)
                                tabAndKey
                        tid <-
                            SBFM.liftWithIndexNamed
                                noStateIdx
                                "aux/fst"
                                (pureUnitSf . fst)
                                tabAndKey
                        p <- SBFM.prepareTable serIdx compIdx encIdx tab
                        SBFM.lift3WithIndexNamed
                            storeIdx
                            "pipeline/store-table"
                            storeTable
                            db
                            tid
                            p
                SBFM.smap f tables)
  where
    noStateIdx = 0
    serIdx = 1
    compIdx = 2
    encIdx = 3
    storeIdx = 4

fbmWritePipeline :: RawWritePipeline
fbmWritePipeline tables =
    get >>= \KVSState {..} ->
        void $
        liftIO $
        flip
            FBM.runOhuaM
            [FBM.toS _serializer, FBM.toS _compression, s, FBM.toS _encryption] $
        FBM.smap
            (\(tid, t) ->
                 FBM.store serIdx compIdx encIdx storeIdx _storage tid t)
            tables
  where
    s = FBM.toS ()
    serIdx = 0
    compIdx = 1
    storeIdx = 2
    encIdx = 3

-- configs that work:
-- genTables = 300
-- keyCount = 100
-- fieldCount = 100
-- at a latency of 600000 I get:
-- ("pipeline/store-table",    27998186667),
-- ("prepare-store/compress",  31196806667),
-- ("prepare-store/encrypt",   13689993333),
-- ("prepare-store/serialize", 30687693333)
-- still I barely get a speedup of about 2x for FBM:
-- Running fbm with ... --> numCores: 1
-- 10125
-- Running fbm with ... --> numCores: 4
-- 7629
-- Running fbm with ... --> numCores: 3
-- 6988
-- Running fbm with ... --> numCores: 2
-- 7881
--
-- SBFM (MVars) does quite good though:
--
-- Running sbfm with ... --> numCores: 1
-- 13769
-- Running sbfm with ... --> numCores: 4
-- 8343
-- Running sbfm with ... --> numCores: 3
-- 7076
-- Running sbfm with ... --> numCores: 2
-- 6523
-- ===> However, I never got a 3-fold speedup!
-- (Why is the exec time 3s shorter in FBM?)
--
-- some more tries:
--
-- Running fbm with ... --> numCores: 1
-- 11928
-- Running fbm with ... --> numCores: 4
-- 6800
-- Running fbm with ... --> numCores: 3
-- 6784
-- Running fbm with ... --> numCores: 2
-- 7417
--
-- Running fbm with ... --> numCores: 1
-- 11647
-- Running fbm with ... --> numCores: 4
-- 7122
-- Running fbm with ... --> numCores: 3
-- 6820
-- Running fbm with ... --> numCores: 2
-- 6889


-- ---> reducing the latency to 500 gives me the following results:
-- Running fbm with ... --> numCores: 1
-- 8231
-- Running fbm with ... --> numCores: 4
-- 4850
-- Running fbm with ... --> numCores: 3
-- 4653
-- Running fbm with ... --> numCores: 2
-- 4322

-- Running sbfm with ... --> numCores: 1
-- 8721
-- Running sbfm with ... --> numCores: 4
-- 4639
-- Running sbfm with ... --> numCores: 3
-- 3797
-- Running sbfm with ... --> numCores: 2
-- 4634

-- --> both do equally well, but FBM is better without the delay for some reason!

---------------------
--
-- genTables = 600
-- keyCount = 100
-- fieldCount = 50
--
-- genTables = 1200
-- keyCount = 50
-- fieldCount = 50
-- not as good as above but still work:
-- genTables = 20
-- keyCount = 300
-- fieldCount = 400

-- configs that do not work as well:
-- genTables = 100
-- keyCount = 100
-- fieldCount = 100
--
-- genTables = 50
-- keyCount = 100
-- fieldCount = 100
--

testPipeline :: Int -> Int -> [Int] -> IO [[(String, Integer)]]
testPipeline numEntries numFields cores = do
  setStdGen (mkStdGen 20)
  -- putStrLn " --> Generating tables ..."
  tables <- evalRandIO $ genTables 300
  forceA_ tables
  -- liftIO $ evaluate $ force tables
  -- putStrLn " --> done."
  mapM (runTest tables) cores
  where
    runTest tables cores = do
        setNumCapabilities cores
        (times, dbs) <-
            unzip <$>
            mapM
                (uncurry $ runSystem cores tables)
                --,
                [
                -- ("pure", pureWritePipeline)
                -- Options:
                -- flip evalStateT (0::Int) . --> needs runtime option -qm
                -- runChanM .
                -- runParIO .
                 ( "sbfm-chan", do liftIO .
                                    writeFile "stats" .
                                    show .
                                    pure @[] .
                                    ("pipeline", ) . map (first show) . Map.toList <=<
                                    sbfmWritePipeline runChanM )
                -- ,
                -- FIXME fails with "thread blocked indefinitely on an MVar operation"
                 -- ( "sbfm-par", do liftIO .
                 --                    writeFile "stats" .
                 --                    show .
                 --                    pure @[] .
                 --                    ("pipeline", ) . map (first show) . Map.toList <=<
                 --                    sbfmWritePipeline runParIO )
                 -- ("fbm", fbmWritePipeline)
                ]
        pure times
      -- let namedDbs = zip (map fst times) dbs
      -- let pureDB = head dbs
      -- forM_ (tail namedDbs) $ \(name, db) ->
      --   unless (db == pureDB) $ printf "Database for %v is unequal to pure database\n" name
      -- let variants = length (nub dbs)
      -- unless (variants == 1) $ error $ printf "Inequality in databases: %d versions" variants
    rInt :: MonadRandom m => m Int
    rInt = getRandom
    -- keyCount = 20
    -- fieldCount = 20
    keyCount = numEntries
    fieldCount = numFields
    genTables = flip replicateM genTable
    genTable =
        curry ((T.pack . tableTemplate) *** HM.fromList) <$> rInt <*>
        replicateM keyCount genKVPair
    genKVPair =
        curry ((T.pack . keyTemplate) *** HM.fromList) <$> rInt <*>
        replicateM fieldCount genFields
    genFields =
        curry ((T.pack . fieldTemplate) *** (T.pack . valueTemplate)) <$> rInt <*>
        rInt
    getDB = readIORef . _dbRef . _storage
    forceDB state = getDB state >>= forceA
    runSystem numCores tables sys f = do
        -- putStrLn $ "num requests: " ++ (show $ length tables)
        s0 <- initState True
        -- let s = s0 { _storage = MockDB (_dbRef $ _storage s0) 0 12000000 }
        let s = s0 { _storage = MockDB (_dbRef $ _storage s0) 0 1 }
        -- hPrintf stderr "Running %s with ..." (sys :: String)
        -- putStrLn $ " --> numCores: " ++ (show numCores)
        -- hFlush stderr
        -- just to create a pattern in the trace
        liftIO performGC
        liftIO performGC
        liftIO performGC
        t0 <- currentTimeMillis
        -- putStrLn $ " --> starting computation: " ++ (show t0)
        runStateT (f tables) s
        -- putStrLn " --> after runStateT."
        forceDB s
        t1 <- currentTimeMillis
        -- putStrLn $ " --> finished computation: " ++ (show t1)
        let time = t1 - t0
        -- hPrintf stderr "%v\n" (show time)
        db <- getDB s
        pure ((sys, time), db)


data Recorded = Recorded { cores :: Int, size :: Int , sysAndTimes :: HM.HashMap String [Integer] } deriving Generic

data GCBenchConfig = GCBenchConfig { minSize :: Int, maxSize :: Int, stepSize :: Int, minCores :: Int, maxCores :: Int } deriving Generic

deriveJSON defaultOptions ''Recorded
deriveJSON defaultOptions ''GCBenchConfig

benchmarkGC :: IO ()
benchmarkGC = do
  config <- AE.decode <$> BS.readFile "./gc-bench-config.json"
  (GCBenchConfig minS maxS stepS minCores maxCores) <- case config of
                Nothing -> putStrLn "Using default config!" >> (return $ GCBenchConfig 10 100 10 1 4)
                Just c -> return c
  results <- mapM (runPipeline minCores maxCores) [minS,(minS+stepS)..maxS]
  BS.writeFile "results.json" $ AE.encode results
  return ()
  where
    runPipeline minCores maxCores size = forM [minCores..maxCores] $ runPipelineForCore size
    runPipelineForCore size cores = do
      putStrLn $ "Running config: #cores = " ++ (show cores) ++ " size = " ++ (show size)
      times <- replicateM 2 $ (return . join) =<< testPipeline size size [cores]
      times' <- return $ join times
      let sysAndTimes = HM.fromListWith (++) [ (sys, [time]) | (sys, time) <- times' ]
      return $ Recorded cores size sysAndTimes

main :: IO ()
main =
  -- profileRequests
  -- testEachAction 10
  -- benchMain
  -- putStrLn ("Microbenchmark num caps: " ++ (show numCapabilities)) >>
  benchmarkGC >>
  -- testPipeline 50 50 [1..4] >>
  return ()

benchMain :: IO ()
benchMain = do
     conf <- either error id . AE.eitherDecode <$> BS.hGetContents stdin
     BS.putStr .
         AE.encode =<<
         let ?execRequests = execFn (systemVersion conf)
          in runMultipleBatches conf
