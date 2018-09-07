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
import Microbenchmark.Common


showState :: KVSState MockDB -> IO String
showState KVSState {_cache = cache, _storage = MockDB dbRef _ _} = do
    db <- readIORef dbRef
    return $ "Cache:\n" ++ show cache ++ "\nDB:\n" ++ show db



defaultTableCount = 20



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
