{-# LANGUAGE TypeSynonymInstances, FlexibleInstances, InstanceSigs, LambdaCase #-}

module Kvstore.Serialization where

import           Kvstore.KVSTypes
import qualified Data.Aeson           as AE
import           Data.ByteString.Lazy
import           Data.Maybe
import           GHC.Generics
import           Control.DeepSeq
import LazyObject

import           Debug.Trace

mkStatelessSer :: (Table -> ByteString) -> Serialization
mkStatelessSer f = Serialization (\() tbl -> (f tbl, ())) ()

mkStatelessDeser :: (ByteString -> Table) -> Deserialization
mkStatelessDeser f = Deserialization (\() tbl -> (f tbl, ())) ()

serialize :: Serialization -> Table -> (Serialization, ByteString)
serialize (Serialization ser s) tbl = let (bs, s') = ser s tbl in (Serialization ser s', bs)

deserialize :: Deserialization -> ByteString -> (Deserialization, Table)
deserialize (Deserialization de s) bs = let (tbl, s') = de s bs in (Deserialization de s', tbl)

jsonDecodeThrowing :: AE.FromJSON a => ByteString -> a
jsonDecodeThrowing = either error id . AE.eitherDecode

jsonSer :: Serialization
jsonSer = mkStatelessSer (AE.encode . fmap LazyObject.read)

jsonDeSer :: Deserialization
jsonDeSer = mkStatelessDeser (fmap newChanged . jsonDecodeThrowing)
