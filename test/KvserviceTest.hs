{-# LANGUAGE LambdaCase #-}
import Test.Framework
import Test.Framework.Providers.HUnit
import Test.HUnit hiding (State)

import qualified Kvstore.KeyValueService as KVS
import qualified Kvstore.Ohua.FBM.KeyValueService as KVSOhuaFBM
import qualified Kvstore.Ohua.SBFM.KeyValueService as KVSOhuaSBFM

import qualified CorrectnessTests as CT (buildSuite)

-- DELETE AGAIN
-- import qualified Data.ByteString.Lazy.Char8             as B8
--
-- import qualified Kvstore.InputOutput               as InOut
-- import           Debug.Trace
-- import           ServiceConfig
-- import           Kvstore.KVSTypes
-- import           Control.Monad.State
--
-- main :: IO ()
-- main = evalStateT compute KVSState{ _compression=zlibComp, _decompression=zlibDecomp }
--   where
--     compute = do
--        one <- InOut.compressTable $ B8.pack "{\"key-0\":{\"field-0\":\"value-0\"}}"
--        two <- InOut.compressTable $ B8.pack "my-second-test-string"
--        resOne <- B8.unpack <$> InOut.decompressTable one
--        resTwo <- B8.unpack <$> InOut.decompressTable two
--        traceM $ "result one: " ++ resOne
--        traceM $ "result two: " ++ resTwo
-- import Crypto
-- import qualified Data.ByteString.Char8             as B8
--
-- main :: IO ()
-- main = exampleAES256 $ B8.pack "my stupid test string"
runSuite s = defaultMainWithOpts s mempty

main :: IO ()
-- main = do
--     options <-
--         execParser
--             (info
--                  (config <**> helper)
--                  (fullDesc <>
--                   progDesc
--                       "A micro service for key-value storage that automatically replicates data across multiple DBs and keeps a cache for fast access." <>
--                   header "KVStore"))
--     runSuite $
--         case testOrBench options of
--             "mb" -> MB.buildSuite $ bmConfig options
--             _ -> CT.buildSuite

main = runSuite CT.buildSuite
