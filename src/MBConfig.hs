{-# LANGUAGE TemplateHaskell, OverloadedStrings #-}
module MBConfig (BatchConfig(..), def, Operation(..)) where

import Data.Aeson
import Data.Aeson.TH
import Versions
import Data.Word
import Kvservice_Types

data BatchConfig = BatchConfig
  { keyCount :: Int
  , batchCount :: Int
  , batchSize :: Int
  , useEncryption :: Bool
  , numTables :: Int
  , threadCount :: Int
  , systemVersion :: Version
  , numFields :: Int
  , requestSelection :: Maybe Operation
  , preloadCache :: Bool
  , readDelay :: Word64
  , writeDelay :: Word64
  }


deriveJSON defaultOptions ''Version
deriveJSON defaultOptions ''Operation
deriveJSON defaultOptions { fieldLabelModifier = camelTo2 '_' } ''BatchConfig


def :: BatchConfig
def =
    BatchConfig
        { keyCount = 100
        , batchCount = 30
        , batchSize = 30
        , useEncryption = False
        , numTables = 10
        , threadCount = 1
        , systemVersion = Functional
        , numFields = 5
        , requestSelection = Nothing
        , preloadCache = False
        , readDelay = 0
        , writeDelay = 0
        }
