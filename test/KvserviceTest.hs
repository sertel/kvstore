
import Test.HUnit hiding (State)
import Test.Framework
import Test.Framework.Providers.HUnit

import Options.Applicative
import Data.Semigroup ((<>))

import qualified Kvstore.KeyValueService   as KVS
import qualified Kvstore.Ohua.FBM.KeyValueService   as KVSOhuaFBM

import qualified CorrectnessTests as CT (buildSuite)
import qualified Microbenchmark as MB (buildSuite)

newtype Config = Config String

config :: Parser Config
config = Config
      <$> strOption
          ( short 's'
         <> long "suite"
         <> metavar "SUITE"
         <> help "Test suite to be executed."
         <> showDefault
         <> value "ct" )

runSuite s = defaultMainWithOpts s mempty

main :: IO ()
main = do
  options <- execParser (info (config <**> helper) (fullDesc <> progDesc "A micro service for key-value storage that automatically replicates data across multiple DBs and keeps a cache for fast access."
                                                             <> header "KVStore"))
  runSuite $ case options of { (Config "mb" ) ->  MB.buildSuite; _ ->  CT.buildSuite }
