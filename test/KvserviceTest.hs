{-# LANGUAGE ImplicitParams #-}

import Test.HUnit hiding (State)
import Test.Framework
import Test.Framework.Providers.HUnit

import Options.Applicative
import Data.Semigroup ((<>))

import qualified Kvstore.KeyValueService   as KVS
import qualified Kvstore.Ohua.FBM.KeyValueService   as KVSOhuaFBM

import qualified CorrectnessTests as CT (suite)
import qualified Microbenchmark as MB (suite)

suite arg = case arg of
  "mb" -> MB.suite
  _ -> CT.suite

versions s =
  [
      let ?execRequests = KVS.execRequestsCoarse            in testGroup "\nCoarse-grained imperative version" $ suite s "coarse-grained imperative"
  ,
      let ?execRequests = KVS.execRequestsFine              in testGroup "\nFine-grained imperative version" $ suite s "fine-grained imperative"
  ,
      let ?execRequests = KVS.execRequestsFuncImp           in testGroup "\nFunctional-imperative version" $ suite s "functional-imperative"
  ,
      let ?execRequests = KVS.execRequestsFunctional        in testGroup "\nPurely functional version" $ suite s "purely functional"
  ,
      let ?execRequests = KVSOhuaFBM.execRequestsFunctional in testGroup "\nOhua - FBM version" $ suite s "Ohua - FBM"
  ]

runSuite s = defaultMainWithOpts (versions s) mempty

runSuiteSequentially s = defaultMainWithOpts [mutuallyExclusive $ testGroup "\nSequential run" (versions s)] mempty

data Config = Config String String

config :: Parser Config
config = Config
      <$> strOption
          ( short 's'
         <> long "suite"
         <> metavar "SUITE"
         <> help "Test suite to be executed."
         <> showDefault
         <> value "ct" )
      <*> strOption
          ( short 'e'
         <> long "execution"
         <> metavar "EXECUTION"
         <> help "Execution mode of the test suite."
         <> showDefault
         <> value "par" )

main :: IO ()
-- main = runSuite
-- main = runSuiteSequentially
main = do
  options <- execParser (info (config <**> helper) (fullDesc <> progDesc "A micro service for key-value storage that automatically replicates data across multiple DBs and keeps a cache for fast access."
                                                             <> header "KVStore"))
  case options of
    (Config s "seq") -> runSuiteSequentially s
    (Config s "par") -> runSuite s
    (Config s _) -> runSuite s
