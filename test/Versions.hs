
module Versions where

import qualified Kvstore.KeyValueService   as KVS
import qualified Kvstore.Ohua.FBM.KeyValueService   as KVSOhuaFBM
import           Requests

versions :: [(ExecReqFn, String)]
versions =
  [ (KVS.execRequestsCoarse,            "coarse-grained imperative")
  , (KVS.execRequestsFine,              "fine-grained imperative")
  , (KVS.execRequestsFuncImp,           "functional-imperative")
  , (KVS.execRequestsFunctional,        "purely functional")
  , (KVSOhuaFBM.execRequestsFunctional, "ohua - FBM")
  ]
