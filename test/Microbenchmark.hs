{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ImplicitParams #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables, NamedFieldPuns,
  OverloadedStrings, FlexibleContexts, PartialTypeSignatures #-}

import Control.Lens
import Control.DeepSeq
import Control.Monad
import Control.Monad.State
import Control.Arrow (first, second)
import System.Random
import Data.Void
import Control.DeepSeq
import System.Exit
import System.IO
import System.Mem
import System.CPUTime

import GHC.Conc.Sync (setNumCapabilities)

import Data.Aeson as AE
import Data.Aeson.TH as AE
import qualified Data.ByteString.Lazy as BS
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as Set
import qualified Data.Map as Map
import Data.IORef
import Data.List
import Data.Time.Clock.POSIX
import qualified Data.Vector as V
import Kvservice_Types
import Kvstore.KVSTypes
import Kvstore.Serialization

import qualified Kvstore.Ohua.SBFM.KeyValueService as KVS

import Requests
import ServiceConfig
import Versions
import MBConfig

import Statistics.Sample (mean)

import Debug.Trace
import Text.Printf


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
            (MockDB db 0) -- latency is 0 for loading the DB and then
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
showState KVSState {_cache = cache, _storage = MockDB dbRef _} = do
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
  -- traceM "requ

loadDB useEncryption tc keyCount = do
    s <- initState useEncryption
  -- fill the db first
    requests <- fmap V.concat $ forM [0..tc - 1] $ \i ->
        evalStateT (workload keyCount)
        defaultBenchmarkState
            { _operationSelection = RangeGen 0 0 $ mkStdGen 0 -- (INSERT only)
            , _tableSelection = RangeGen i i $ mkStdGen 0
            }
  -- traceM "requests (INSERT):"
  -- mapM (\i -> traceM $ show i ++ "\n" ) requests
    (responses, s'@KVSState {_storage = (MockDB db _)}) <-
        flip runStateT s $ ?execRequests requests
  -- adjust minLatency for benchmark requests
    return s' {_storage = MockDB db 50}
  -- traceM "state after init:"
  -- traceM =<< showState s'
  -- traceM "done with insert."

reqBenchmarkState keyCount numTables =
    defaultBenchmarkState
        { _keySelection = RangeGen 1 keyCount $ mkStdGen 0
        , _operationSelection = RangeGen 0 4 $ mkStdGen 0 -- (RangeGen 1 3 $ mkStdGen 0) -- _operationSelection (no INSERT, no DELETE)
        , _tableCount = numTables
        , _tableSelection = RangeGen 0 numTables $ mkStdGen 0
        }

currentTimeMillis = round . (* 1000) <$> getPOSIXTime

-- then run some requests
runRequests ::
       (?execRequests :: ExecReqFn)
    => Int
    -> Int
    -> BenchmarkState RangeGen
    -> StateT (KVSState MockDB) IO Integer
runRequests operationCount _ bmState = do
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
                               } = do
    s <- loadDB useEncryption numTables keyCount
    let bmState = reqBenchmarkState keyCount numTables
    execTimes <-
        flip evalStateT s $
        sequence $ replicate batchCount (runRequests batchSize keyCount bmState)
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



testEachAction replications = do
    results <- sequence $ replicate replications runTest :: IO [Map.Map String (Map.Map _ Integer)]
    let avg l = sum l `div` toInteger (length l)
        stats = fmap (fmap avg) (Map.unionsWith (Map.unionWith mappend) $ map (fmap $ fmap pure) results :: Map.Map String (Map.Map _ [Integer]))
    writeFile
        "stat-results"
        (show $ fmap (second (fmap (first show) . Map.toList)) $ Map.toList stats)
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


main :: IO ()
main = benchMain -- testEachAction 10


benchMain :: IO ()
benchMain = do
     conf <- either error id . AE.eitherDecode <$> BS.hGetContents stdin
     BS.putStr .
         AE.encode =<<
         let ?execRequests = execFn (systemVersion conf)
          in runMultipleBatches conf
