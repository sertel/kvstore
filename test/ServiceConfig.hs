{-# LANGUAGE InstanceSigs, FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveAnyClass #-}

module ServiceConfig where

import qualified Data.Text.Lazy            as T
import qualified Data.HashMap.Strict       as HM
import           Data.IORef

import qualified DB_Iface                  as DB
import           Db_Types
import           Debug.Trace

import           Kvstore.KVSTypes

import           Codec.Compression.Zlib
import           Control.DeepSeq
import           GHC.Generics

type MockDB = IORef (HM.HashMap T.Text T.Text)

instance DB.DB_Iface MockDB where
  get :: MockDB -> T.Text -> IO DBResponse
  get dbRef key = DBResponse . HM.lookup key <$> readIORef dbRef

  put :: MockDB -> T.Text -> T.Text -> IO ()
  put dbRef key value = do
    db <- readIORef dbRef
    -- traceM $ "key: " ++ show key
    let convert = \case (Just p) -> p; Nothing -> T.empty
    let db' = HM.insert key value db
    writeIORef dbRef db'

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
