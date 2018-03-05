{-# LANGUAGE ImplicitParams #-}

import Test.HUnit hiding (State)
import Test.Framework
import Test.Framework.Providers.HUnit

import qualified Kvstore.KeyValueService   as KVS
import qualified Kvstore.Ohua.FBM.KeyValueService   as KVSOhuaFBM

import CorrectnessTests (suite)
-- import Microbenchmark (suite)

runSuite =
  defaultMainWithOpts
    (
       (let ?execRequests = KVS.execRequestsCoarse            in suite "coarse-grained imperative")
    ++ (let ?execRequests = KVS.execRequestsFine              in suite "fine-grained imperative")
    ++ (let ?execRequests = KVS.execRequestsFuncImp           in suite "functional-imperative")
    ++ (let ?execRequests = KVS.execRequestsFunctional        in suite "purely functional")
    ++ (let ?execRequests = KVSOhuaFBM.execRequestsFunctional in suite "Ohua - FBM")
    )
    mempty

main :: IO ()
main = runSuite
