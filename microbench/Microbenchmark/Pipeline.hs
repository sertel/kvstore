{-# LANGUAGE TemplateHaskell, OverloadedLabels, PartialTypeSignatures #-}
{-# LANGUAGE BangPatterns, ScopedTypeVariables,
  NamedFieldPuns, OverloadedStrings, FlexibleContexts,
  RecordWildCards, TypeApplications, ImplicitParams, TupleSections,
  RankNTypes, DeriveGeneric, DataKinds, TypeOperators, ViewPatterns,
  LambdaCase, ConstraintKinds, TypeFamilies, GADTs #-}
module Microbenchmark.Pipeline where


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

import Microbenchmark.Common



sbfmWritePipeline ::
       MonadStream m
    => (m _ -> IO _)
    -> [(T.Text, Table)]
    -> StateT (KVSState MockDB) IO _
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

runners =
    [ ("pure", pureWritePipeline)
            -- Options:
            -- flip evalStateT (0::Int) . --> needs runtime option -qm
            -- runChanM .
            -- runParIO .
    , ("sbfm-chan", void . sbfmWritePipeline runChanM)
            -- ,
            -- FIXME fails with "thread blocked indefinitely on an MVar operation"
    , ("sbfm-par", void . sbfmWritePipeline runParIO)
    , ("fbm", fbmWritePipeline)
    , ("default", pureWritePipeline)
    ]

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
        s0 <- initState ! #lazySerialization False ! #useEncryption True
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



data BenchParams = BenchParams { file :: String }

cmdArgParser :: Parser BenchParams
cmdArgParser = BenchParams
              <$> strOption ( long "file"
                           <> metavar "f"
                           <> help "Benchmark config file" )

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
