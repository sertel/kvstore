{-# LANGUAGE PartialTypeSignatures, DeriveAnyClass,
  OverloadedLabels, TemplateHaskell #-}

import Control.Arrow (first, second)
import Control.Monad.Random
import Control.DeepSeq
import Control.Lens
import Control.Monad.State (MonadState, StateT, evalStateT, get, runStateT, modify, execStateT)
import Data.Aeson as AE hiding (Array)
import Data.Aeson.TH as AE
import qualified Data.ByteString.Lazy as BS
import Data.Dynamic2
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as Set
import Data.IORef
import Data.List (nub)
import qualified Data.Map as Map
import qualified Data.Text.Lazy as T
import Data.Time.Clock.POSIX
import qualified Data.Vector as V
import GHC.Conc.Sync (setNumCapabilities)
import Kvservice_Types
import Kvstore.InputOutput (store, load)
import Kvstore.KVSTypes
import Kvstore.Serialization
import Data.Maybe (fromJust,fromMaybe, catMaybes)
import System.CPUTime
import System.IO
import System.Mem
import Named
import Named.Internal (Param(Param))
import Data.Word
import Data.Foldable
import Data.Semigroup ((<>))

import Criterion
import Criterion.Types (Measured(measTime))
import Criterion.Measurement (measure, initializeTime)

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
import DB_Iface (DB_Iface)

import Requests
import ServiceConfig
import Versions
import MBConfig

import Statistics.Sample (mean)

import Debug.Trace
import Text.Printf

import GHC.Generics

import Options.Applicative


type NamedTable = (T.Text, Table)

forceA :: (NFData a, Applicative f) => a -> f a
forceA a = a `deepseq` pure a

forceA_ :: (NFData a, Applicative f) => a -> f ()
forceA_ a = a `deepseq` pure ()

data Bounds a = Bounds ("lowerBound" :! a) ("upperBound" :! a)

maxBounds :: Bounded a => Bounds a
maxBounds = Bounds ! #lowerBound minBound ! #upperBound maxBound

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

valueTemplate, fieldTemplate, keyTemplate, tableTemplate :: Show a => a -> String
valueTemplate v = "value-" ++ show v
fieldTemplate f = "field-" ++ show f
keyTemplate k = "key-" ++ show k
tableTemplate t = "table-" ++ show t


named :: name :! a -> Param (name :! a)
named = Param
{-# INLINE named #-}

data BenchmarkState = BenchmarkState
    { fieldCount :: Int
    , fieldSelection :: Bounds Int
    , valueSizeBounds :: Bounds Int
    , tableCount :: Int
    , tableSelection :: Bounds Int
    , keySelection :: Bounds Int
    , operationSelection :: Maybe [Operation]
    , fieldCountSelection :: Bounds (Int)
    , scanCountSelection :: Bounds Int
    }

makeLenses ''BenchmarkState

-- lens does not work with RankNTypes :(
createValue :: (Num a, Show a, MonadRandom m, Enum a) => Bounds a -> m String
createValue valueSizeBounds = randomEnumFromBounds valueSizeBounds <&> \i -> mconcat $ map valueTemplate [1..succ i]

randomFrom :: (MonadRandom m, Foldable f) => f a -> m a
randomFrom = uniform

randomEnum :: (MonadRandom m, Enum a) => "lowerBound" :! a -> "upperBound" :! a -> m a
randomEnum (Arg lo) (Arg hi) =
    getRandom <&> \i -> toEnum $ loI + (fromIntegral $ i `mod` range)
  where
    loI = fromEnum lo
    hiI = fromEnum hi
    range = hiI - loI


randomEnumFromBounds :: (MonadRandom m, Enum a) => Bounds a -> m a
randomEnumFromBounds (Bounds lo hi) = randomEnum ! named lo ! named hi

randomBounded :: (MonadRandom m, Enum a, Bounded a) => m a
randomBounded = randomEnum ! #lowerBound minBound ! #upperBound maxBound

createRequest :: MonadRandom m
    => BenchmarkState -> m KVRequest
createRequest BenchmarkState {..} = do
        op <- maybe randomBounded randomFrom operationSelection
        table <- tableTemplate <$> randomEnumFromBounds tableSelection
        key <- keyTemplate <$> randomEnumFromBounds keySelection
        case op of
            INSERT -> prepareINSERT table key <$> createINSERTEntry
            UPDATE -> prepareUPDATE table key <$> createUPDATEEntry
            READ -> prepareREAD table key . Set.map fieldTemplate <$> getFields
            SCAN ->
                prepareSCAN table key <$> getScanCount <*>
                (Set.map fieldTemplate <$> getFields)
            DELETE -> return $ prepareDELETE table key
    -- let opSelector = view operationSelection bmState
    --     (op, opSelector') = next opSelector
    --     bmState' = over operationSelection (const opSelector') bmState
    --     tableSelector = view tableSelection bmState
    --     (tableId, tableSelector') = next tableSelector
    --     table = tableTemplate tableId
    --     bmState'' = over tableSelection (const tableSelector') bmState'
    --     keySelector = view keySelection bmState
    --     (keyId, keySelector') = next keySelector
    --     key = keyTemplate keyId
    --     bmState''' = over keySelection (const keySelector') bmState''
    -- put bmState'''
    -- case op of
    --     0 -> prepareINSERT table key <$> createINSERTEntry
    --     1 -> prepareUPDATE table key <$> createUPDATEEntry
    --     2 -> prepareREAD table key . Set.map fieldTemplate <$> getFields
    --     3 ->
    --         prepareSCAN table key <$> getScanCount <*>
    --         (Set.map fieldTemplate <$> getFields)
    --     4 -> return $ prepareDELETE table key
    --     _ -> error $ "No such operation: " ++ show op
  where
    getFieldsAndValues =
        fmap HM.fromList .
        mapM (\i -> (,) <$> pure (fieldTemplate i) <*> createValue valueSizeBounds)
    createINSERTEntry = getFieldsAndValues [1 .. succ fieldCount]
    getFields = do
        aFieldCount <- randomEnumFromBounds fieldCountSelection
        Set.fromList <$> replicateM aFieldCount (randomEnumFromBounds fieldSelection)
    createUPDATEEntry = getFieldsAndValues . Set.toList =<< getFields
    getScanCount =
        randomEnumFromBounds scanCountSelection

workload :: MonadRandom m => Int -> BenchmarkState -> m (V.Vector KVRequest)
workload operationCount state = V.fromList <$> replicateM operationCount (createRequest state)

showState :: KVSState MockDB -> IO String
showState KVSState {_cache = cache, _storage = MockDB dbRef _ _} = do
    db <- readIORef dbRef
    return $ "Cache:\n" ++ show cache ++ "\nDB:\n" ++ show db

defaultFieldCount :: Int
defaultFieldCount = 10

defaultBenchmarkState =
    BenchmarkState
        { fieldCount = defaultFieldCount
        , fieldSelection = Bounds ! #lowerBound 0 ! #upperBound 10
        , valueSizeBounds = Bounds ! #lowerBound 5 ! #upperBound 10
        , tableCount = 20
        , tableSelection = Bounds ! #lowerBound 1 ! #upperBound 1
        , keySelection = maxBounds
        , operationSelection = Nothing
        , fieldCountSelection = Bounds ! #lowerBound 3 ! #upperBound 10
        , scanCountSelection = Bounds ! #lowerBound 5 ! #upperBound 10
        }
  -- traceM "recur

loadDB ::
       "numTables" :! _
    -> "numKeys" :! _
    -> "numFields" :? _
    -> StateT (KVSState MockDB) IO ()
loadDB (named -> tc) (named -> keyCount) (argDef #numFields defaultFieldCount -> numFields) = do
    tables <-
        liftIO $ genTables ! keyCount ! #numFields numFields ! tc
    pureWritePipeline tables
  -- traceM "state after init:"
  -- traceM =<< showState s'
  -- traceM "done with insert."

reqBenchmarkState keyCount numTables fieldCount opSelect =
    defaultBenchmarkState
        { keySelection = Bounds ! #lowerBound 1 ! #upperBound keyCount
        , operationSelection = opSelect
              -- (RangeGen 1 3 $ mkStdGen 0) -- _operationSelection (no INSERT, no DELETE)
        , tableCount = numTables
        , tableSelection = Bounds ! #lowerBound 0 ! #upperBound numTables
        , fieldCount = fieldCount
        , fieldCountSelection = Bounds ! #lowerBound 0 ! #upperBound fieldCount
        }

currentTimeMillis = round . (* 1000) <$> getPOSIXTime

-- then run some requests
runRequests ::
       (?execRequests :: ExecReqFn)
    => "operationCount" :! Int
    -> "preloadCache" :! Bool
    -> BenchmarkState
    -> StateT (KVSState MockDB) IO Double
runRequests (Arg operationCount) (Arg preload) bmState = do
    liftIO $ traceEventIO "Building requests"
    let requests = flip evalRand (mkStdGen 0) $ workload operationCount bmState
  -- traceM $ "requests:"
  -- mapM (\i -> traceM $ show i ++ "\n" ) requests
    forceA_ requests
    when preload $ liftIO (traceEventIO "Preloading cache") >> doPreload requests
    liftIO $ traceEventIO "Forcing GC"
    liftIO performGC
    liftIO $ traceEventIO "Starting measurement"
    s <- get
    let bench = nfIO $ flip evalStateT s $ ?execRequests requests
    (result, _) <- liftIO $ measure bench 1
    let execTime = measTime result
    liftIO $ traceEventIO "Finished measurement"
  -- traceM "???????????????????????????????????????"
  -- traceM $ "responses:"
  -- mapM (\i -> traceM $ show i ++ "\n" a) responses
    -- unless (operationCount == length responses) $ liftIO $
    --   error "wrong number of responses"
    return execTime

doPreload :: DB_Iface db => V.Vector KVRequest -> StateT (KVSState db) IO ()
doPreload requests = do
    tables <- catMaybes <$> mapM load tableIds
    mapM_ forceA tables
    cache' <- use cache
    let !cache'' = foldr' (uncurry HM.insert) cache' tables
    cache Control.Lens..= cache''
  where
    tableIds = nub $ V.toList $ fmap kVRequest_table requests

defaultTableCount = 20

setDelay :: MonadState (KVSState MockDB) m => "readDelay" :! Word64 -> "writeDelay" :! Word64 -> m ()
setDelay (Arg readDelay) (Arg writeDelay) =
    modify $ \s@KVSState {_storage = MockDB db _ _} ->
        s {_storage = MockDB db readDelay writeDelay}


runMultipleBatches ::
       (?execRequests :: ExecReqFn) => BatchConfig -> IO Double
runMultipleBatches BatchConfig { useEncryption = useEncryption
                               , keyCount
                               , batchCount
                               , batchSize
                               , numTables
                               , numFields
                               , requestSelection
                               , preloadCache
                               , readDelay
                               , writeDelay
                               } = do
    traceEventIO "Starting benchmark"
    let bmState =
            reqBenchmarkState keyCount numTables numFields (pure <$> requestSelection)
    s <- initState useEncryption
    execTimes <-
        flip evalStateT s $ do
            liftIO $ traceEventIO "Loading Database"
            loadDB ! #numTables numTables
                   ! #numKeys keyCount
                   ! #numFields numFields
            liftIO $ traceEventIO "Setting delay"
            setDelay ! #readDelay readDelay
                     ! #writeDelay writeDelay
            liftIO $ traceEventIO "Done loading!"
            replicateM
                batchCount
                (runRequests ! #operationCount batchSize
                             ! #preloadCache preloadCache
                             $ bmState)
    let meanExecTime = mean $ V.fromList execTimes
   -- traceM $ "mean execution time: " ++ show meanExecTime ++ " ms"
    return meanExecTime

avg :: (Foldable f, Num i, Integral i) => f i -> i
avg l = sum l `div` fromIntegral (length l)

testEachAction replications = do
    results <-
        replicateM replications runTest :: IO [Map.Map String (Map.Map _ Integer)]
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
        s <- initState True
        flip runStateT s $ do
            loadDB ! #numKeys 10 ! #numTables 100 ! defaults
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
    s <- initState True
    s' <- flip execStateT s $ loadDB ! #numTables numTables ! #numKeys keyCount ! #numFields fieldCount
    t0 <- currentTimeMillis
    stats <-
        flip evalStateT s' $
        replicateM 5 $ do
            requests <-
                liftIO $ workload operationCount bmState
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

runners = [ ("pure", pureWritePipeline)
            -- Options:
            -- flip evalStateT (0::Int) . --> needs runtime option -qm
            -- runChanM .
            -- runParIO .
           , ( "sbfm-chan", do liftIO .
                                 writeFile "stats" .
                                 show .
                                 pure @[] .
                                 ("pipeline", ) . map (first show) . Map.toList <=<
                                 sbfmWritePipeline runChanM )
            -- ,
            -- FIXME fails with "thread blocked indefinitely on an MVar operation"
           , ( "sbfm-par", do liftIO .
                                 writeFile "stats" .
                                 show .
                                 pure @[] .
                                 ("pipeline", ) . map (first show) . Map.toList <=<
                                 sbfmWritePipeline runParIO )
           , ("fbm", fbmWritePipeline)
           , ("default", pureWritePipeline)
           ]


genTables ::
       MonadRandom m
    => "numKeys" :! Int
    -> "numFields" :! Int
    -> "numTables" :! Int
    -> m [NamedTable]
genTables (Arg keyCount) (Arg fieldCount) (Arg tableCount) =
    replicateM tableCount genTable
  where
    genTable =
        curry (bimap (T.pack . tableTemplate) HM.fromList) <$> rInt <*>
        replicateM keyCount genKVPair
    genKVPair =
        curry (bimap (T.pack . keyTemplate) HM.fromList) <$> rInt <*>
        replicateM fieldCount genFields
    genFields =
        curry (bimap (T.pack . fieldTemplate) (T.pack . valueTemplate)) <$> rInt <*>
        rInt
    rInt :: MonadRandom m => m Int
    rInt = randomBounded

testPipeline ::
        "numEntries" :! Int
     -> "numFields" :! Int
     -> "cores" :! [Int]
     -> String
     -> IO [Integer]
testPipeline (Arg numEntries) (Arg numFields) (Arg cores) benchRunner = do
  -- putStrLn " --> Generating tables ..."
  let tables = flip evalRand (mkStdGen 20) $
                  genTables ! #numTables 300
                            ! #numKeys numEntries
                            ! #numFields numFields
  forceA_ tables
  -- liftIO $ evaluate $ force tables
  -- putStrLn " --> done."
  mapM (runTest tables) cores
  where
    runTest tables cores = do
        setNumCapabilities cores
        (execTime, _dbs) <-
            runSystem cores tables $ fromJust $ lookup benchRunner runners
        pure execTime
      -- let namedDbs = zip (map fst times) dbs
      -- let pureDB = head dbs
      -- forM_ (tail namedDbs) $ \(name, db) ->
      --   unless (db == pureDB) $ printf "Database for %v is unequal to pure database\n" name
      -- let variants = length (nub dbs)
      -- unless (variants == 1) $ error $ printf "Inequality in databases: %d versions" variants
    -- keyCount = 20
    -- fieldCount = 20
    getDB = liftIO . readIORef . _dbRef . _storage
    forceDB = getDB >=> forceA_
    runSystem _numCores tables f = do
        -- putStrLn $ "num requests: " ++ (show $ length tables)
        s0 <- initState True
        -- let s = s0 { _storage = MockDB (_dbRef $ _storage s0) 0 12000000 }
        let s = s0 { _storage = MockDB (_dbRef $ _storage s0) 0 1 }
        -- hPrintf stderr "Running %s with ..." (sys :: String)
        -- putStrLn $ " --> numCores: " ++ (show numCores)
        -- hFlush stderr
        -- just to create a pattern in the trace
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
        pure (time, db)


data Recorded = Recorded
    { cores :: Int
    , size :: Int
    , benchmark :: String
    , execTimes :: [Integer]
    } deriving (Generic, Show)

data GCBenchConfig = GCBenchConfig
    { runner :: String
    , minSize :: Int
    , maxSize :: Int
    , stepSize :: Int
    , minCores :: Int
    , maxCores :: Int
    , reps :: Int
    } deriving (Generic, Show)

deriveJSON defaultOptions ''Recorded
deriveJSON defaultOptions ''GCBenchConfig

benchmarkGC :: BenchParams -> IO ()
benchmarkGC (BenchParams configFile) = do
    configs <-
        fromMaybe [GCBenchConfig "default" 10 100 10 1 4 2] . AE.decode <$>
        BS.readFile configFile
    putStrLn $ "benchmark configs: " ++ (show configs)
    results <- mapM runPipeBenchmark configs
    BS.writeFile (configFile ++ "-results.json") $ AE.encode $ join results
    return ()
  where
    runPipeBenchmark GCBenchConfig {..} =
        sequence
            [ do putStrLn $
                     "Running config: #cores = " ++
                     show cores ++ " size = " ++ show size
                 times <-
                     replicateM reps $
                     (testPipeline ! #numEntries size ! #numFields size !
                      #cores [cores])
                         runner
                 return $ Recorded cores size runner (join times)
            | size <- [minSize,(minSize + stepSize) .. maxSize]
            , cores <- [minCores .. maxCores]
            ]

data BenchParams = BenchParams { file :: String }

cmdArgParser :: Parser BenchParams
cmdArgParser = BenchParams
              <$> strOption ( long "file"
                           <> metavar "f"
                           <> help "Benchmark config file" )


main :: IO ()
main =
  hSetBuffering stdout LineBuffering >> -- needed for running @ ZIH
  -- profileRequests
  -- testEachAction 10
  -- putStrLn ("Microbenchmark num caps: " ++ (show numCapabilities)) >>
  benchMain
  --execParser opts >>= void . benchmarkGC
  where
    opts = info (cmdArgParser <**> helper)
      ( fullDesc
     <> progDesc "Run the write-pipeline against the supplied config."
     <> header "Pipeline MicroBenchmark" )



benchMain :: IO ()
benchMain = do
    initializeTime
    conf <- either error id . AE.eitherDecode <$> BS.hGetContents stdin
    BS.putStr . AE.encode =<<
        let ?execRequests = execFn (systemVersion conf)
         in runMultipleBatches conf
