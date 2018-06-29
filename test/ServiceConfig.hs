{-# LANGUAGE InstanceSigs, FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveAnyClass, BangPatterns #-}

module ServiceConfig where

import qualified Data.Text.Lazy            as T
import qualified Data.HashMap.Strict       as HM
import qualified Data.ByteString.Lazy      as BS
import           Data.IORef
import Data.Hashable

import qualified DB_Iface                  as DB
import           Db_Types
import           Debug.Trace

import           Kvstore.KVSTypes

import           Codec.Compression.GZip
import           Control.DeepSeq
import           Control.Concurrent (threadDelay)
import           GHC.Generics

import           Crypto
import GHC.Stack
import Control.Monad

import Data.Binary as Ser
import Data.Binary.Orphans ()

import Data.Serialize as Cereal
import Data.Serialize.Text

import Kvstore.Ohua.Cache (forceLazyByteString)

data MockDB = MockDB {
      _dbRef :: IORef (HM.HashMap T.Text BS.ByteString)
    , _readLatency :: Int
    , _writeLatency :: Int
    } deriving Generic
deriving instance NFData MockDB

instance DB.DB_Iface MockDB where
  get :: MockDB -> T.Text -> IO DBResponse
  get (MockDB dbRef readLatency _) key = do
    -- traceM $ "getting data for key: " ++ show key
    resp <- DBResponse . HM.lookup key <$> readIORef dbRef
    unless (readLatency == 0) $ threadDelay readLatency
    return resp

  put :: MockDB -> T.Text -> BS.ByteString -> IO ()
  put (MockDB dbRef _ writeLatency) key value = do
    forceLazyByteString value `deepseq` pure ()
    db <- readIORef dbRef
    -- traceM $ "key: " ++ show key
    -- let convert = \case (Just p) -> p; Nothing -> T.empty
    let db' = HM.insert key value db
    writeIORef dbRef db'
    unless (writeLatency == 0) $ threadDelay writeLatency

deriving instance Generic CompressParams
deriving instance NFData CompressParams
deriving instance NFData CompressionLevel
deriving instance NFData Method
deriving instance NFData WindowBits
deriving instance NFData MemoryLevel
deriving instance NFData CompressionStrategy

deriving instance Generic DecompressParams
instance NFData DecompressParams

zlibComp :: Compression
zlibComp = flip Compression defaultCompressParams { compressLevel = compressionLevel 7 }
                            $ \s t -> let c = compressWith s t
                                      in (c,s)
-- zlibComp = flip Compression ()
--                             $ \s t -> let c = compress t
--                                       in (c,s)

zlibDecomp :: Decompression
zlibDecomp = flip Decompression defaultDecompressParams
                                $ \s t -> let d = decompressWith s t
                                          in (d,s)
-- zlibDecomp = flip Decompression ()
--                                 $ \s t -> let d = decompress t
--                                           in (d,s)

noComp :: Compression
noComp = Compression (\s t -> (t, s)) ()

noDecomp :: Decompression
noDecomp = Decompression (\s t -> (t, s)) ()

aesEncryption :: HasCallStack => IO (Encryption, Decryption)
aesEncryption = do
  aesState <- initAESState
  let e = flip Encryption aesState
                          $ \s@(AESState sk iv) t ->
                              case (encrypt sk iv (BS.toStrict t)) of
                                    Left err -> error $ show err
                                    Right eMsg -> (BS.fromStrict eMsg,s)
      d = flip Decryption aesState
                          $ \s@(AESState sk iv) t ->
                                  case decrypt sk iv (BS.toStrict t) of
                                        Left err -> error $ show err
                                        Right eMsg -> (BS.fromStrict eMsg,s)
  return (e,d)

noEnc :: Encryption
noEnc = Encryption (\s t -> (t,s)) ()

noDec :: Decryption
noDec = Decryption (\s t -> (t,s)) ()

binarySerialization :: Serialization
binarySerialization = Serialization (\() tbl -> (Ser.encode tbl, ())) ()

binaryDeserialization :: Deserialization
binaryDeserialization = Deserialization (\() tbl -> (Ser.decode tbl, ())) ()

instance (Hashable key, Eq key, Cereal.Serialize key, Cereal.Serialize value) => Cereal.Serialize (HM.HashMap key value) where
    get = HM.fromList <$> Cereal.get
    put = Cereal.put . HM.toList

cerealSerialization :: Serialization
cerealSerialization =  Serialization (\() tbl -> (BS.fromStrict $ Cereal.encode tbl, ())) ()

cerealDeserialization :: Deserialization
cerealDeserialization =  Deserialization (\() tbl -> (either error id $ Cereal.decode $ BS.toStrict tbl, ())) ()
