{-# LANGUAGE TemplateHaskell, OverloadedStrings #-}
module MBConfig where

import Data.Aeson
import Data.Aeson.TH
import Versions

data BatchConfig = BatchConfig
  { keyCount :: Int
  , batchCount :: Int
  , batchSize :: Int
  , useEncryption :: Bool
  , numTables :: Int
  , threadCount :: Int
  , systemVersion :: Version
  , numFields :: Int
  , requestSelection :: Maybe Int
  }


deriveJSON defaultOptions ''Version
deriveJSON defaultOptions { fieldLabelModifier = camelTo2 '_' } ''BatchConfig
