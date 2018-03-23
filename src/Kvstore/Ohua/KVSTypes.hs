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

instance DS.NFData Operation
instance DS.NFData KVResponse

instance NFData Serialization where
  rnf (Serialization _ st) = rnf st

instance NFData Deserialization where
  rnf (Deserialization _ st) = rnf st

instance NFData Encryption where
  rnf (Encryption _ st) = rnf st

instance NFData Decryption where
  rnf (Decryption _ st) = rnf st

instance NFData Compression where
  rnf (Compression _ st) = rnf st

instance NFData Decompression where
  rnf (Decompression _ st) = rnf st

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
calcUpdateState = ()
calcUpdateStateIdx = 5 :: Int
calcInsertState = ()
calcInsertStateIdx = 6 :: Int
updateSerializeTableState :: Serialization -> Serialization
updateSerializeTableState = id
updateSerializeTableStateIdx = 7 :: Int
insertSerializeTableState :: Serialization -> Serialization
insertSerializeTableState = id
insertSerializeTableStateIdx = 8 :: Int
deleteSerializeTableState :: Serialization -> Serialization
deleteSerializeTableState = id
deleteSerializeTableStateIdx = 9 :: Int

insertStoreTableState = ()
insertStoreTableStateIdx = 10 :: Int
deleteStoreTableState = ()
deleteStoreTableStateIdx = 11 :: Int
updateStoreTableState = ()
updateStoreTableStateIdx = 12 :: Int

loadTableState = ()
loadTableStateIdx = 13 :: Int
foldINSERTsIntoCacheState = ()
foldINSERTsIntoCacheStateIdx = 14 :: Int

decryptTableState :: Decryption -> Decryption
decryptTableState = id
decryptTableStateIdx = 15 :: Int
decompressTableState :: Decompression -> Decompression
decompressTableState = id
decompressTableStateIdx = 16 :: Int

insertCompressTableState :: Compression -> Compression
insertCompressTableState = id
insertCompressTableStateIdx = 17 :: Int
insertEncryptTableState :: Encryption -> Encryption
insertEncryptTableState = id
insertEncryptTableStateIdx = 18 :: Int

updateCompressTableState :: Compression -> Compression
updateCompressTableState = id
updateCompressTableStateIdx = 19 :: Int
updateEncryptTableState :: Encryption -> Encryption
updateEncryptTableState = id
updateEncryptTableStateIdx = 20 :: Int

deleteCompressTableState :: Compression -> Compression
deleteCompressTableState = id
deleteCompressTableStateIdx = 21 :: Int
deleteEncryptTableState :: Encryption -> Encryption
deleteEncryptTableState = id
deleteEncryptTableStateIdx = 22 :: Int

lastStateIdx = deleteEncryptTableStateIdx

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
                    , toS calcUpdateState
                    , toS calcInsertState
                    , toS $ updateSerializeTableState ser
                    , toS $ insertSerializeTableState ser
                    , toS $ deleteSerializeTableState ser
                    , toS insertStoreTableState
                    , toS deleteStoreTableState
                    , toS updateStoreTableState
                    , toS loadTableState
                    , toS foldINSERTsIntoCacheState

                    , toS $ decryptTableState dec
                    , toS $ decompressTableState decomp

                    , toS $ insertCompressTableState comp
                    , toS $ insertEncryptTableState enc
                    , toS $ updateCompressTableState comp
                    , toS $ updateEncryptTableState enc
                    , toS $ deleteCompressTableState comp
                    , toS $ deleteEncryptTableState enc
                    ]


convertState :: [SE.S] -> (Serialization, Deserialization, Compression, Decompression, Encryption, Decryption)
convertState s = ( fromS $ s !! updateSerializeTableStateIdx
                 , fromS $ s !! deserializeTableStateIdx
                 , fromS $ s !! insertCompressTableStateIdx
                 , fromS $ s !! decompressTableStateIdx
                 , fromS $ s !! insertEncryptTableStateIdx
                 , fromS $ s !! decryptTableStateIdx
                 )
-- FIXME this is only true when the SerDe is stateless.
-- otherwise the API here can not cope with having many states, one for each of the SerDes.
-- we will need to change the Kvstore.Ohua.FBM.KeyValueService.execRequestsFunctional
-- function and the defined KVSState.
