{-# LANGUAGE LambdaCase #-}
module Versions where

import qualified Kvstore.KeyValueService            as KVS
import qualified Kvstore.Ohua.FBM.KeyValueService   as KVSOhuaFBM
import qualified Kvstore.Ohua.SBFM.KeyValueService  as KVSOhuaSBFM
import qualified Data.Vector as V
import           Kvstore.KVSTypes
import           Kvservice_Types
import Control.Monad.State
import Control.DeepSeq
import Data.Typeable
import DB_Iface (DB_Iface)

type ExecReqFn_ a = V.Vector KVRequest -> StateT (KVSState a) IO (V.Vector KVResponse)

data Version
  = Imperative_coarseGrained
  | Imperative_fineGrained
  | Imperative_functional
  | Functional
  | Ohua_FBM
  | Ohua_SBFM
  deriving (Enum, Bounded, Show)

execFn :: (DB_Iface db, NFData db, Typeable db) => Version -> ExecReqFn_ db
execFn = \case
  Imperative_coarseGrained -> KVS.execRequestsCoarse
  Imperative_fineGrained -> KVS.execRequestsFine
  Imperative_functional -> KVS.execRequestsFuncImp
  Functional -> KVS.execRequestsFunctional
  Ohua_FBM -> KVSOhuaFBM.execRequestsFunctional
  Ohua_SBFM -> KVSOhuaSBFM.execRequestsFunctional
