{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Kvstore.Ohua.KVSTypes where

import           Kvservice_Types
import           Kvstore.KVSTypes
import qualified Control.DeepSeq            as DS
import           GHC.Generics
import qualified DB_Iface                   as DB
import           Data.StateElement          as SE
import           Data.Typeable
import qualified Data.ByteString.Lazy       as BS
import           Control.DeepSeq

import           Debug.Trace


--
-- Global State definition
--

type Stateless = ()

foldIntoCacheState = ()
foldIntoCacheStateIdx = 0 :: Int
foldEvictFromCacheState = ()
foldEvictFromCacheStateIdx = 1 :: Int
deserializeTableState :: Deserialization -> Deserialization
deserializeTableState = id
deserializeTableStateIdx = 2 :: Int
rEADReqHandlingState = ()
rEADReqHandlingStateIdx = 3 :: Int
sCANReqHandlingState = ()
sCANReqHandlingStateIdx = 4 :: Int

loadTableState = ()
loadTableStateIdx = 5 :: Int

decryptTableState :: Decryption -> Decryption
decryptTableState = id
decryptTableStateIdx = 6 :: Int
decompressTableState :: Decompression -> Decompression
decompressTableState = id
decompressTableStateIdx = 7 :: Int

writebackCompressTableStateIdx = 8 :: Int
writebackEncryptTableStateIdx = 9 :: Int
writebackSerializeTableStateIdx = 10 :: Int
writebackStoreTableStateIdx = 11 :: Int

foldWritesIntoCacheIdx = 12 :: Int

lastStateIdx = foldWritesIntoCacheIdx

-- FIXME state sharing not ok at this level
globalState :: Serialization -> Deserialization ->
               Compression -> Decompression ->
               Encryption -> Decryption ->
               [SE.S]
globalState ser deser comp decomp enc dec =
                    [
                      toS foldIntoCacheState
                    , toS foldEvictFromCacheState
                    , toS $ deserializeTableState deser
                    , toS rEADReqHandlingState
                    , toS sCANReqHandlingState
                    , toS loadTableState

                    , toS $ decryptTableState dec
                    , toS $ decompressTableState decomp

                    , toS comp -- writeback compression
                    , toS enc -- writeback encryption
                    , toS ser -- writeback serialization
                    , toS ()

                    , toS ()
                    ]


convertState :: [SE.S] -> (Serialization, Deserialization, Compression, Decompression, Encryption, Decryption)
convertState s = ( fromS $ s !! writebackSerializeTableStateIdx
                 , fromS $ s !! deserializeTableStateIdx
                 , fromS $ s !! writebackCompressTableStateIdx
                 , fromS $ s !! decompressTableStateIdx
                 , fromS $ s !! writebackEncryptTableStateIdx
                 , fromS $ s !! decryptTableStateIdx
                 )
-- FIXME this is only true when the SerDe is stateless.
-- otherwise the API here can not cope with having many states, one for each of the SerDes.
-- we will need to change the Kvstore.Ohua.FBM.KeyValueService.execRequestsFunctional
-- function and the defined KVSState.
