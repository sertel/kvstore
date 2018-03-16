
module TestSetup where

import qualified Data.HashMap.Strict       as HM
import           Data.IORef

import           Kvstore.Serialization
import           Kvstore.KVSTypes

import           Kvservice_Types
import           ServiceConfig

initState :: IO (KVSState MockDB)
initState = do
  db <- newIORef HM.empty
  return $ KVSState HM.empty (db :: MockDB) jsonSer jsonDeSer undefined undefined undefined undefined
