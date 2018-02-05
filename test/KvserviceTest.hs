{-# LANGUAGE InstanceSigs, FlexibleInstances #-}

import Test.HUnit hiding (State)
import Test.Framework
import Test.Framework.Providers.HUnit

import qualified Data.Text.Lazy         as T
import qualified Data.HashMap.Strict    as HM
import qualified Data.Vector            as V
import           Data.Maybe

import qualified DB_Iface               as DB

type MockDB = HM.HashMap T.Text T.Text

instance DB.DB_Iface MockDB where
  get :: MockDB -> T.Text -> IO T.Text
  get db key = return $ fromJust $ HM.lookup key db

  put :: MockDB -> T.Text -> T.Text -> IO T.Text
  put db key value =
    let
      convert = \x -> case x of { (Just p) -> p; Nothing -> T.empty }
      prev = (convert . HM.lookup key) db
      _ = HM.insert key value db
    in
      return prev

simpleTest :: Assertion
simpleTest = do
  let
    req = HM.singleton "table-0" $ HM.singleton "key-0" $ HM.singleton "field-0" "value-0"
    reqs = V.singleton req
    (responses, state) = runIO . (runStateT (execRequests req)) $ KVSState
                                                                    KVStore
                                                                    MockDB

  -- let (result,state) = runOhuaM (simpleComposition 333 10) [0,0]
  -- assertEqual "result was wrong." 36 result
  -- assertEqual "state was wrong." [2,3] state
  return undefined


main :: IO ()
main = defaultMainWithOpts
       [
        testCase "running some requests against the kvs" simpleTest
       ]
       mempty
