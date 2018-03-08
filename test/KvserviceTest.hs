{-# LANGUAGE ImplicitParams #-}

import Test.HUnit hiding (State)
import Test.Framework
import Test.Framework.Providers.HUnit

import qualified Kvstore.KeyValueService   as KVS
import qualified Kvstore.Ohua.FBM.KeyValueService   as KVSOhuaFBM

import CorrectnessTests (suite)
-- import Microbenchmark (suite)

versions =
  [
      let ?execRequests = KVS.execRequestsCoarse            in testGroup "\nCoarse-grained imperative version" $ suite "coarse-grained imperative"
  ,
      let ?execRequests = KVS.execRequestsFine              in testGroup "\nFine-grained imperative version" $ suite "fine-grained imperative"
  ,
      let ?execRequests = KVS.execRequestsFuncImp           in testGroup "\nFunctional-imperative version" $ suite "functional-imperative"
  ,
      let ?execRequests = KVS.execRequestsFunctional        in testGroup "\nPurely functional version" $ suite "purely functional"
  ,
      let ?execRequests = KVSOhuaFBM.execRequestsFunctional in testGroup "\nOhua - FBM version" $ suite "Ohua - FBM"
  ]

runSuite = defaultMainWithOpts versions mempty

runSuiteSequentially = defaultMainWithOpts [mutuallyExclusive $ testGroup "\nSequential run" versions] mempty

main :: IO ()
-- main = runSuite
main = runSuiteSequentially
