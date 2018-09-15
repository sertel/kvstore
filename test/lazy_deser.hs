{-# LANGUAGE OverloadedLabels, StandaloneDeriving, DeriveGeneric #-}

import Criterion
import Criterion.Main
import Microbenchmark.Common
import Named
import Data.Binary as Ser
import GHC.Generics
import Control.DeepSeq

import Kvstore.KVSTypes

import Data.HashMap.Strict (HashMap)
import Data.ByteString.Lazy (ByteString)
import Data.Text.Lazy (Text)

genTable :: IO Table
genTable = fmap (snd . head) $ genTables ! #numKeys 70 ! #numFields 70 ! #numTables 1

type PartiallySerializedTable = HashMap Text ByteString


decodeDirect :: ByteString -> Table
decodeDirect = Ser.decode

decodePartial :: ByteString -> PartiallySerializedTable
decodePartial = Ser.decode

main =
    defaultMain
        [ env genTable $ \ tbl->
              bgroup
                  "serialization"
                  [ bench "direct" $ nf Ser.encode (tbl :: Table)
                  , bench "partial" $
                    let input = fmap Ser.encode tbl :: PartiallySerializedTable
                     in input `deepseq` nf Ser.encode input
                  ]
        , env ((\tbl -> (Ser.encode tbl, Ser.encode $ fmap Ser.encode tbl)) <$>
               genTable) $ \ ~(direct, partial) ->
              bgroup
                  "deserialization"
                  [ bench "direct whnf" $ whnf decodeDirect direct
                  , bench "direct nf" $ nf decodeDirect direct
                  , bench "partial whnf" $ whnf decodePartial partial
                  , bench "partial nf" $ nf decodePartial partial
                  ]
        ]
