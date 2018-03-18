{-# LANGUAGE InstanceSigs, FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveAnyClass #-}

module ServiceConfig where

import qualified Data.Text.Lazy            as T
import qualified Data.HashMap.Strict       as HM
import qualified Data.ByteString.Lazy      as BS
import           Data.IORef

import qualified DB_Iface                  as DB
import           Db_Types
import           Debug.Trace

import           Kvstore.KVSTypes

import           Codec.Compression.Zlib
import           Control.DeepSeq
import           Control.Concurrent (threadDelay)
import           GHC.Generics

import           Crypto


data MockDB = MockDB {
      _dbRef :: IORef (HM.HashMap T.Text T.Text)
    , _minLatency :: Int } deriving Generic
deriving instance NFData MockDB

instance DB.DB_Iface MockDB where
  get :: MockDB -> T.Text -> IO DBResponse
  get (MockDB dbRef minLatency) key = do
    resp <- DBResponse . HM.lookup key <$> readIORef dbRef
    case minLatency of { 0 -> return (); _ -> threadDelay minLatency }
    return resp

  put :: MockDB -> T.Text -> T.Text -> IO ()
  put (MockDB dbRef minLatency) key value = do
    db <- readIORef dbRef
    -- traceM $ "key: " ++ show key
    let convert = \case (Just p) -> p; Nothing -> T.empty
    let db' = HM.insert key value db
    writeIORef dbRef db'
    case minLatency of { 0 -> return (); _ -> threadDelay minLatency }

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
zlibComp = flip Compression defaultCompressParams
                            $ \s t -> let c = compressWith s t
                                      in (c,s)

zlibDecomp :: Decompression
zlibDecomp = flip Decompression defaultDecompressParams
                                $ \s t -> let d = decompressWith s t
                                          in (d,s)

noComp :: Compression
noComp = Compression (\s t -> (t, s)) ()

noDecomp :: Decompression
noDecomp = Decompression (\s t -> (t, s)) ()

aesEncryption :: IO (Encryption, Decryption)
aesEncryption = do
  aesState <- initAESState
  let e = flip Encryption aesState
                          $ \s@(AESState sk iv) t ->
                              case encrypt sk iv (BS.toStrict t) of
                                    Left err -> error $ show err
                                    Right eMsg -> (BS.fromStrict eMsg,s)
      d = flip Decryption aesState
                          $ \s@(AESState sk iv) t ->
                                  case decrypt sk iv (BS.toStrict t) of
                                        Left err -> error $ show err
                                        Right eMsg -> (BS.fromStrict eMsg,s)
  return (e,d)

noEnc :: Encryption
noEnc = Encryption (\s t -> (t, s)) ()

noDec :: Decryption
noDec = Decryption (\s t -> (t, s)) ()
