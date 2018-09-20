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
import           System.CPUTime.Rdtsc (rdtsc)
import Control.Exception
import LazyObject
import Kvstore.Serialization

import           Crypto
import GHC.Stack
import Control.Monad

import Data.Binary as Ser
import Data.Binary.Orphans ()

import Data.Serialize as Cereal
import Data.Serialize.Text

import Kvstore.Ohua.Cache (forceLazyByteString)

import System.CPUTime
import System.IO (stderr)

import Text.Printf

type RawDB = IORef (HM.HashMap T.Text BS.ByteString)
data MockDB = MockDB {
      _dbRef :: RawDB
    , _readLatency :: Word64
    , _writeLatency :: Word64
    } deriving Generic
deriving instance NFData MockDB

makeNoWait :: RawDB -> MockDB
makeNoWait db = MockDB db 0 0

make :: RawDB -> Word64 -> Word64 -> IO MockDB
make db 0 0 = pure $ MockDB db 0 0
make db readDelay writeDelay = do
  cost <- measureSin 10000
  let adjust 0 = 0
      adjust i = round $ (realToFrac i / realToFrac cost :: Double) * 10000
      wd = adjust writeDelay
      rd = adjust readDelay
  hPrintf stderr "Calculated a read delay of %d and a write delay of %d" rd wd
  pure $ MockDB db rd wd

wait_sins :: Word64 -> Int -> IO Float
wait_sins num node = evaluate $ sin_iter num (2.222 + fromIntegral node)

-- Measure the cost of N Sin operations.
measureSin :: Word64 -> IO Word64
measureSin n = do
    t0 <- rdtsc
    res <- evaluate (sin_iter n 38.38)
    t1 <- rdtsc
    return $ t1 - t0

sin_iter :: Word64 -> Float -> Float
sin_iter 0  x = x
sin_iter n !x = sin_iter (n - 1) (x + sin x)

instance DB.DB_Iface MockDB where
  get :: MockDB -> T.Text -> IO DBResponse
  get (MockDB dbRef readLatency _) key = do
    -- traceM $ "getting data for key: " ++ show key
    db <- readIORef dbRef
    let resp = DBResponse $ HM.lookup key db
    unless (readLatency == 0) $ void $ wait_sins readLatency (HM.size db)
    return resp

  put :: MockDB -> T.Text -> BS.ByteString -> IO ()
  put (MockDB dbRef _ writeLatency) key value = do
    forceLazyByteString value `deepseq` pure ()
    db <- readIORef dbRef
    -- traceM $ "key: " ++ show key
    -- let convert = \case (Just p) -> p; Nothing -> T.empty
    let db' = HM.insert key value db
    writeIORef dbRef db'
    unless (writeLatency == 0) $ void $ wait_sins writeLatency (HM.size db)

busyWait :: Int -> IO ()
busyWait i = do
  ref <- newIORef 0
  zero <- getCPUTime
  let loop = do
        t <- getCPUTime
        if t - (toInteger $ i * 1000) < zero
          then do
            res <- readIORef ref
            res `seq` pure ()
          else replicateM_ 1000 (modifyIORef' ref (+1)) >> loop
  loop

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
zlibComp = flip Compression defaultCompressParams { compressLevel = compressionLevel 2 }
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
binarySerialization = mkStatelessSer (Ser.encode . fmap LazyObject.read)

binaryDeserialization :: Deserialization
binaryDeserialization = mkStatelessDeser (fmap newChanged . Ser.decode)

instance (Hashable key, Eq key, Cereal.Serialize key, Cereal.Serialize value) => Cereal.Serialize (HM.HashMap key value) where
    get = HM.fromList <$> Cereal.get
    put = Cereal.put . HM.toList

-- cerealSerialization :: Serialization
-- cerealSerialization =  Serialization (\() tbl -> (BS.fromStrict $ Cereal.encode tbl, ())) ()

-- cerealDeserialization :: Deserialization
-- cerealDeserialization =  Deserialization (\() tbl -> (either error id $ Cereal.decode $ BS.toStrict tbl, ())) ()

lazyBinarySerialization :: Serialization
lazyBinarySerialization = mkStatelessSer (Ser.encode . fmap (encodeObject Ser.encode))

lazyBinaryDeserialization :: Deserialization
lazyBinaryDeserialization = mkStatelessDeser (fmap (new Ser.decode) . Ser.decode)
