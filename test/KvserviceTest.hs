
import           Test.Framework
import           Test.Framework.Providers.HUnit
import           Test.HUnit                        hiding (State)

import           Data.Semigroup                    ((<>))
import           Options.Applicative

import qualified Kvstore.KeyValueService           as KVS
import qualified Kvstore.Ohua.FBM.KeyValueService  as KVSOhuaFBM
import qualified Kvstore.Ohua.SBFM.KeyValueService as KVSOhuaSBFM

import qualified CorrectnessTests                  as CT (buildSuite)
import qualified Microbenchmark                    as MB (BenchmarkConfig,
                                                          benchmarkOptionsParser,
                                                          buildSuite)

data Config = Config { testOrBench :: String
                     , bmConfig    :: MB.BenchmarkConfig }

config :: Parser Config
config = Config
      <$> strOption
          ( short 's'
         <> long "suite"
         <> metavar "SUITE"
         <> help "Test suite to be executed."
         <> showDefault
         <> value "ct" )
      <*> MB.benchmarkOptionsParser

runSuite s = defaultMainWithOpts s mempty

main :: IO ()
main = do
  options <- execParser (info (config <**> helper) (fullDesc <> progDesc "A micro service for key-value storage that automatically replicates data across multiple DBs and keeps a cache for fast access."
                                                             <> header "KVStore"))
  runSuite $ case testOrBench options of { "mb" ->  MB.buildSuite $ bmConfig options; _ ->  CT.buildSuite }
