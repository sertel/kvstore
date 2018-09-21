{-# LANGUAGE OverloadedLabels, BangPatterns, ScopedTypeVariables,
  NamedFieldPuns, OverloadedStrings, FlexibleContexts,
  RecordWildCards, TypeApplications, ImplicitParams, TupleSections,
  RankNTypes, DeriveGeneric, DataKinds, TypeOperators, ViewPatterns,
  LambdaCase, ConstraintKinds, TypeFamilies, GADTs #-}
module Microbenchmark.KVStore where


import Control.Monad.Random
import Control.Lens
import Control.Monad.State (MonadState, StateT, evalStateT, get, modify)
import Data.Aeson as AE hiding (Array)
import qualified Data.ByteString.Lazy as BS
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as Set
import Data.List (nub)
import qualified Data.Vector as V
import Kvservice_Types
import Kvstore.InputOutput (load)
import Kvstore.KVSTypes
import Data.Maybe (catMaybes)
import System.IO
import System.Mem
import Named
import Data.Word
import Data.Foldable
import Text.Printf

import Criterion
import Criterion.Types (Measured(measTime))
import Criterion.Measurement (measure, initializeTime)

import DB_Iface (DB_Iface)

import Requests
import ServiceConfig
import Versions
import MBConfig

import Statistics.Sample (mean)

import Debug.Trace
import LazyObject as L

import Microbenchmark.Common


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
  where
    getFieldsAndValues =
        fmap HM.fromList .
        mapM
            (\i ->
                 (,) <$> pure (fieldTemplate i) <*> createValue valueSizeBounds)
    createINSERTEntry = getFieldsAndValues [1 .. succ fieldCount]
    getFields = do
        aFieldCount <- randomEnumFromBounds fieldCountSelection
        Set.fromList <$>
            replicateM aFieldCount (randomEnumFromBounds fieldSelection)
    createUPDATEEntry = getFieldsAndValues . Set.toList =<< getFields
    getScanCount = randomEnumFromBounds scanCountSelection

workload :: MonadRandom m => Int -> BenchmarkState -> m (V.Vector KVRequest)
workload operationCount state = V.fromList <$> replicateM operationCount (createRequest state)

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
    liftIO $ hPrintf stderr "Loaded %i tables" (length tables)
    cache' <- use cache
    let !cache'' = foldr' (uncurry HM.insert) cache' tables
    cache Control.Lens..= cache''
  where
    tableIds = nub $ V.toList $ fmap kVRequest_table requests


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
                               , lazySerialization
                               } = do
    traceEventIO "Starting benchmark"
    let bmState =
            reqBenchmarkState keyCount numTables numFields (pure <$> requestSelection)
    s <- initState ! #lazySerialization lazySerialization ! #useEncryption useEncryption
    execTimes <-
        flip evalStateT s $ do
            liftIO $ traceEventIO "Loading Database"
            loadDB ! #numTables numTables
                   ! #numKeys keyCount
                   ! #numFields numFields
            liftIO $ traceEventIO "Setting delay"
            calculateDelay ! #readDelay readDelay
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

benchMain :: IO ()
benchMain = do
    initializeTime
    --L.enableStatCollection
    conf <- either error id . AE.eitherDecode <$> BS.hGetContents stdin
    BS.putStr . AE.encode =<<
        let ?execRequests = execFn (systemVersion conf)
         in do
          res <- runMultipleBatches conf
          L.printLazySerUsage
          pure res
