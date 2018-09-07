{-# LANGUAGE PartialTypeSignatures, DeriveAnyClass,
  OverloadedLabels, TemplateHaskell #-}

import Control.Arrow (first, second)
import Control.Monad.Random
import Control.Lens
import Control.Monad.State (evalStateT, runStateT, modify, execStateT)
import Data.IORef
import qualified Data.Map as Map
import Kvstore.KVSTypes
import System.CPUTime
import System.IO
import System.Mem
import Named
import Data.Semigroup ((<>))
import Text.Printf

import qualified Kvstore.Ohua.SBFM.KeyValueService as KVS

import Requests
import ServiceConfig

import Options.Applicative

import Microbenchmark.Pipeline
import Microbenchmark.KVStore
import Microbenchmark.Profile


showState :: KVSState MockDB -> IO String
showState KVSState {_cache = cache, _storage = MockDB dbRef _ _} = do
    db <- readIORef dbRef
    return $ "Cache:\n" ++ show cache ++ "\nDB:\n" ++ show db

data BenchType
  = Profile ProfileType
  | Pipeline BenchParams
  | Batches

benchTypeParser :: Parser BenchType
benchTypeParser =
    hsubparser
        (command
             "profile"
             (info
                  (Profile <$> profileTypeParser)
                  (progDesc
                       "Fine grained profiling for individual requests and whole batches")) <>
         command
             "pipeline"
             (info
                  (Pipeline <$> cmdArgParser)
                  (progDesc "Benchmark the isolated write pipeline")) <>
         command
             "batches"
             (info (pure Batches) (progDesc "Benchmark the kvstore")))

main :: IO ()
main =
    hSetBuffering stdout LineBuffering >> -- needed for running @ ZIH
  -- putStrLn ("Microbenchmark num caps: " ++ (show numCapabilities)) >>
    execParser opts >>= \case
        Profile popts -> profileMain popts
        Batches -> benchMain
        Pipeline popts -> void $ benchmarkGC popts
  where
    opts =
        info
            (benchTypeParser <**> helper)
            (fullDesc <>
             progDesc "Various micro benchmarks for the kvstore and its parts" <>
             header "MicroBenchmark")
