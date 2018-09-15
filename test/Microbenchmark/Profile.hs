{-# LANGUAGE OverloadedLabels, PartialTypeSignatures #-}
{-# LANGUAGE BangPatterns, ScopedTypeVariables,
  NamedFieldPuns, OverloadedStrings, FlexibleContexts,
  RecordWildCards, TypeApplications, ImplicitParams, TupleSections,
  RankNTypes, DeriveGeneric, DataKinds, TypeOperators, ViewPatterns,
  LambdaCase, ConstraintKinds, TypeFamilies, GADTs #-}
module Microbenchmark.Profile where


import Control.Arrow (first, second)
import Control.Concurrent (setNumCapabilities)
import Control.Lens hiding (argument)
import Control.Monad.Random
import Control.Monad.State (evalStateT, execStateT, modify, runStateT)
import Data.Aeson (eitherDecode)
import qualified Data.ByteString.Lazy as BS (readFile)
import Data.IORef
import qualified Data.Map as Map
import Data.Semigroup
import Data.Word (Word64)
import Kvstore.KVSTypes
import Named
import Options.Applicative
import System.CPUTime
import System.Mem
import System.IO (stderr)
import Text.Printf
import qualified Data.Vector as V
import Data.List (nub)
import qualified Data.HashMap.Strict as HM

import qualified Kvstore.Ohua.SBFM.KeyValueService as KVS
import Kvservice_Types

import Requests
import ServiceConfig (_dbRef)

import Microbenchmark.KVStore
import Microbenchmark.Common

import MBConfig

import qualified Foundation as F
import qualified Foundation.Time.StopWatch as F
import qualified Foundation.Time.Types as F

data ProfileType
  = Requests Int
  | Batch (Maybe FilePath)


profileTypeParser :: Parser ProfileType
profileTypeParser =
    hsubparser
        (command
             "requests"
             (info
                  (Requests <$>
                   argument
                       auto
                       (metavar "REPS" <>
                        help "Number of replications to use for profiling"))
                  (progDesc "Profile each request type individually")) <>
         command
             "batch"
             (info
                  (Batch <$>
                   optional
                       (strArgument $
                        metavar "CONFIG" <>
                        help "Path to a configuration file to use for the batch"))
                  (progDesc "Profile an entire batch")))

avg :: (Foldable f, Num i, Integral i) => f i -> i
avg l = sum l `div` fromIntegral (length l)

testEachAction :: Int -> IO ()
testEachAction replications = do
    results <-
        replicateM replications runTest
    let stats =
            fmap
                (fmap avg)
                (Map.unionsWith (Map.unionWith mappend) $
                 map (fmap $ fmap pure) results :: Map.Map String (Map.Map _ [Word64]))
    writeFile
        "stat-results"
        (show $
         second (fmap (first show) . Map.toList) <$> Map.toList stats)
  where
    runTest = do
        statVar <- newIORef mempty
        let execWithCollectStats statname reqs = do
                modify (cache .~ mempty)
                liftIO performGC
                (res, stats) <- KVS.execRequestsFunctional0 reqs
                clock <- liftIO F.startPrecise
                forceA_ res
                F.NanoSeconds forceDone <- liftIO $ F.stopPrecise clock
                let stats' = Map.insert "setup/force-result" forceDone stats
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

profileBatchDefaults :: BatchConfig
profileBatchDefaults = def
  { threadCount = 1
  , useEncryption = True
  }

profileBatch :: BatchConfig -> IO ()
profileBatch BatchConfig {..} = do
    setNumCapabilities threadCount
    s <- initState True
    s' <-
        flip execStateT s $
        loadDB ! #numTables numTables ! #numKeys keyCount ! #numFields numFields
    stats <-
        flip evalStateT s' $ do
            calculateDelay ! #readDelay readDelay ! #writeDelay writeDelay
            replicateM batchCount $ do
                requests <- liftIO $ workload batchSize bmState
                forceA_ requests
                liftIO performGC
                (responses, stats) <- KVS.execRequestsFunctional0 requests
                clock <- liftIO F.startPrecise
                forceA_ responses
                F.NanoSeconds forceDone <- liftIO $ F.stopPrecise clock
                pure $ Map.insert "setup/force-result" forceDone stats
    print
        ([ ( "benchmark"
           , map (first show) $
             Map.toList $
             fmap avg $
             Map.unionsWith mappend $ map (fmap (pure :: a -> [a])) stats)
         ] :: [(String, [(String, Word64)])])
  where
    bmState =
        reqBenchmarkState
            keyCount
            numTables
            numFields
            (pure <$> requestSelection)

profileMain :: ProfileType -> IO ()
profileMain (Batch conf) =
    maybe
        (pure profileBatchDefaults)
        (fmap (either error id . eitherDecode) . BS.readFile)
        conf >>=
    profileBatch
profileMain (Requests reps) = testEachAction reps
