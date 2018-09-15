{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE BangPatterns, ScopedTypeVariables,
  NamedFieldPuns, OverloadedStrings, FlexibleContexts,
  RecordWildCards, TypeApplications, ImplicitParams, TupleSections,
  RankNTypes, DeriveGeneric, DataKinds, TypeOperators, ViewPatterns,
  LambdaCase, ConstraintKinds, TypeFamilies, GADTs #-}
module Microbenchmark.Common where

import Control.DeepSeq
import Control.Lens
import Control.Monad.Random
import Control.Monad.State (MonadState, StateT, modify)
import qualified Data.HashMap.Strict as HM
import Data.IORef
import qualified Data.Text.Lazy as T
import Data.Time.Clock.POSIX
import Data.Word
import Kvstore.InputOutput (store)
import Kvstore.KVSTypes
import Named
import Named.Internal (Param(Param))

import ServiceConfig
import Kvstore.Serialization

type NamedTable = (T.Text, Table)

forceA :: (NFData a, Applicative f) => a -> f a
forceA a = a `deepseq` pure a

forceA_ :: (NFData a, Applicative f) => a -> f ()
forceA_ a = a `deepseq` pure ()

data Bounds a = Bounds ("lowerBound" :! a) ("upperBound" :! a)

maxBounds :: Bounded a => Bounds a
maxBounds = Bounds ! #lowerBound minBound ! #upperBound maxBound

valueTemplate, fieldTemplate, keyTemplate, tableTemplate :: Show a => a -> String
valueTemplate v = "value-" ++ show v
fieldTemplate f = "field-" ++ show f
keyTemplate k = "key-" ++ show k
tableTemplate t = "table-" ++ show t

named :: name :! a -> Param (name :! a)
named = Param
{-# INLINE named #-}


-- lens does not work with RankNTypes :(
createValue :: (Num a, Show a, MonadRandom m, Enum a) => Bounds a -> m String
createValue valueSizeBounds =
    randomEnumFromBounds valueSizeBounds <&> \i ->
        mconcat $ map valueTemplate [1 .. succ i]

randomFrom :: (MonadRandom m, Foldable f) => f a -> m a
randomFrom = uniform

randomEnum :: (MonadRandom m, Enum a) => "lowerBound" :! a -> "upperBound" :! a -> m a
randomEnum (Arg lo) (Arg hi) =
    getRandom <&> \i -> toEnum $ loI + fromIntegral (i `mod` range)
  where
    loI = fromEnum lo
    hiI = fromEnum hi
    range = hiI - loI


randomEnumFromBounds :: (MonadRandom m, Enum a) => Bounds a -> m a
randomEnumFromBounds (Bounds lo hi) = randomEnum ! named lo ! named hi

randomBounded :: (MonadRandom m, Enum a, Bounded a) => m a
randomBounded = randomEnum ! #lowerBound minBound ! #upperBound maxBound


initState :: Bool -> IO (KVSState MockDB)
initState useEncryption = do
    db <- newIORef HM.empty
    (enc, dec) <-
        if useEncryption
            then aesEncryption
            else pure (noEnc, noDec)
    return $
        KVSState
            HM.empty
            (makeNoWait db) -- latency is 0 for loading the DB and then
                          -- we set it for the requests
            -- jsonSer
            binarySerialization
            -- jsonDeSer
            binaryDeserialization
            zlibComp
            zlibDecomp
            enc
            dec

type RawWritePipeline = [(T.Text, Table)] -> StateT (KVSState MockDB) IO ()

pureWritePipeline :: RawWritePipeline
pureWritePipeline = mapM_ $ uncurry store

genTables ::
       MonadRandom m
    => "numKeys" :! Int
    -> "numFields" :! Int
    -> "numTables" :! Int
    -> m [NamedTable]
genTables (Arg keyCount) (Arg fieldCount) (Arg tableCount) =
    mapM genTable [0.. pred tableCount]
  where
    genTable i =
        (T.pack $ tableTemplate i,) . HM.fromList <$>
        replicateM keyCount genKVPair
    genKVPair =
        curry (bimap (T.pack . keyTemplate) HM.fromList) <$> rInt <*>
        replicateM fieldCount genFields
    genFields =
        curry (bimap (T.pack . fieldTemplate) (T.pack . valueTemplate)) <$> rInt <*>
        rInt
    rInt :: MonadRandom m => m Int
    rInt = getRandom

defaultFieldCount :: Int
defaultFieldCount = 10

loadDB ::
       "numTables" :! Int
    -> "numKeys" :! Int
    -> "numFields" :? Int
    -> StateT (KVSState MockDB) IO ()
loadDB (named -> tc) (named -> keyCount) (argDef #numFields defaultFieldCount -> numFields) = do
    let tables =
            flip evalRand (mkStdGen 0) $
            genTables ! keyCount ! #numFields numFields ! tc
    pureWritePipeline tables

setDelay :: MonadState (KVSState MockDB) m => "readDelay" :! Word64 -> "writeDelay" :! Word64 -> m ()
setDelay (Arg readDelay) (Arg writeDelay) =
    modify $ \s@KVSState {_storage = MockDB db _ _} ->
        s {_storage = MockDB db readDelay writeDelay}

calculateDelay :: (MonadIO m, MonadState (KVSState MockDB) m) => "readDelay" :! Word64 -> "writeDelay" :! Word64 -> m ()
calculateDelay (Arg readDelay) (Arg writeDelay) = do
  MockDB db _ _ <- use storage
  db' <- liftIO $ make db readDelay writeDelay
  modify $ \s -> s { _storage = db' }

currentTimeMillis :: IO Integer
currentTimeMillis = round . (* 1000) <$> getPOSIXTime
