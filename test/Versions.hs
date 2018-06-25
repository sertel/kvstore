{-# LANGUAGE LambdaCase #-}
module Versions where

import qualified Kvstore.KeyValueService            as KVS
import qualified Kvstore.Ohua.FBM.KeyValueService   as KVSOhuaFBM
import qualified Kvstore.Ohua.SBFM.KeyValueService  as KVSOhuaSBFM
import           Requests

data Version
  = Imperative_coarseGrained
  | Imperative_fineGrained
  | Imperative_functional
  | Functional
  | Ohua_FBM
  | Ohua_SBFM
  deriving (Enum, Bounded)

execFn :: Version -> ExecReqFn
execFn = \case
  Imperative_coarseGrained -> KVS.execRequestsCoarse
  Imperative_fineGrained -> KVS.execRequestsFine
  Imperative_functional -> KVS.execRequestsFuncImp
  Functional -> KVS.execRequestsFunctional
  Ohua_FBM -> KVSOhuaFBM.execRequestsFunctional
  Ohua_SBFM -> KVSOhuaSBFM.execRequestsFunctional
