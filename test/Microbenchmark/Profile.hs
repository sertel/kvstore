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
import Data.Aeson (eitherDecode, encode)
import qualified Data.ByteString.Lazy as BS (readFile, writeFile)
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
import Monad.StreamsBasedFreeMonad (enableStatCollection)
import Data.Statistics

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
    results
        --replicateM replications
         <-
        runTest
    let stats = snd results
    BS.writeFile "stat-results" (encode stats)
  where
    runTest = do
        statVar <- initStatCollection
        let execWithCollectStats statname reqs = do
                modify (cache .~ mempty)
                liftIO performGC
                (res, stats) <- KVS.execRequestsFunctional0 reqs
                clock <- liftIO F.startPrecise
                forceA_ res
                F.NanoSeconds forceDone <- liftIO $ F.stopPrecise clock
                let stats' = toStat "setup/force-result" [OpCycle 0 0 0 0 forceDone] : stats
                liftIO $
                    atomicModifyIORef statVar ((, ()) . ((statname, stats') :))
                pure res
        s <- initState ! #lazySerialization False ! #useEncryption True
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
        getStats statVar

profileBatchDefaults :: BatchConfig
profileBatchDefaults = def
  { threadCount = 1
  , useEncryption = True
  }

profileBatch :: BatchConfig -> IO ()
profileBatch BatchConfig {..} = do
    setNumCapabilities threadCount
    s <- initState ! #useEncryption useEncryption ! #lazySerialization lazySerialization
    s' <-
        flip execStateT s $
        loadDB ! #numTables numTables ! #numKeys keyCount ! #numFields numFields
    stats <-
        flip evalStateT s' $ do
            calculateDelay ! #readDelay readDelay ! #writeDelay writeDelay
            -- disabling running multiple for now
            -- replicateM batchCount $ do
            id $ do
                requests <- liftIO $ workload batchSize bmState
                forceA_ requests
                liftIO performGC
                (responses, stats) <- KVS.execRequestsFunctional0 requests
                clock <- liftIO F.startPrecise
                forceA_ responses
                F.NanoSeconds forceDone <- liftIO $ F.stopPrecise clock
                let forceStat = toStat [OpStat 0 0 0 0 forceDone]
                pure $ forceStat : stats
    BS.putStrLn $ encode $ head stats
  where
    bmState =
        reqBenchmarkState
            keyCount
            numTables
            numFields
            (pure <$> requestSelection)

profileMain :: ProfileType -> IO ()
profileMain t = do
  enableStatCollection
  case t of
    (Batch conf) ->
      maybe
        (pure profileBatchDefaults)
        (fmap (either error id . eitherDecode) . BS.readFile)
        conf >>=
      profileBatch
    (Requests reps) -> testEachAction reps
