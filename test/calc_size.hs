{-# LANGUAGE OverloadedLabels, FlexibleContexts, BangPatterns #-}

import Microbenchmark.Common
import Control.Monad.Random
import Named
import qualified Data.ByteString.Lazy as BS
import Control.Monad.State (evalStateT)
import Kvstore.InputOutput
import Kvstore.Ohua.Cache (forceLazyByteString)
import qualified Data.Text.Lazy as LT

import GHC.HeapView
import GHC.DataSize
import GHC.AssertNF
import Control.DeepSeq
import qualified Data.HashMap.Strict as M
import Foreign.StablePtr
import Foreign.Ptr
import System.Mem (performGC)

import Text.Printf

recSize :: a -> IO Word
recSize x = do
  performGC
  (result, _) <- recSizeInner (0, mempty) $ asBox x
  -- let g [] = pure ()
  --     g (x:xs) = do
  --       forM_ xs $ \y -> do
  --         e <- areBoxesEqual x y
  --         if e then error "Nooooooo" else pure ()
  --       g xs
  -- g (map snd $ M.elems m)
  pure result

isPresent _ [] = pure False
isPresent r (x:xs) = do
  e <- areBoxesEqual r x
  if e then pure True else isPresent r xs

recSizeInner (!acc, !m) yb@(Box y) = do
    --ptr <- newStablePtr y
    --let ref = toKey ptr
    exists <- isPresent yb m
    if exists
      then pure (acc, m)
      else do
            let m' = yb : m
            size <- closureSize y
            clo <- getBoxedClosureData yb
            foldM recSizeInner (acc + size, m') (allPtrs clo)

asPercent a b = 100 * realToFrac a / realToFrac b :: Double

main = do
    let [(_, d)] =
            evalRand
                (genTables ! #numKeys 200 ! #numFields 200 ! #numTables 1)
                (mkStdGen 0)
    d `deepseq` return ()
    st <- initState True
    let runKV = flip evalStateT st
    () <- do
      serialized <- runKV $ serializeTable d
      compressed <- runKV $ encryptTable =<< compressTable serialized
      printf "%i %i\n" (BS.length serialized) (BS.length compressed)
    assertNF d
    s <- recSize d
    let strings =
            [ s
            | (key, m2) <- M.toList d
            , s <- key : [s2 | (key2, v) <- M.toList m2, s2 <- [key2, v]]
            ] :: [LT.Text]
    s5 <-
        do performGC
           (result, m) <-
               foldM
                   (\a b -> b `deepseq` recSizeInner a (asBox b))
                   (0, mempty)
                   strings
           pure result
    printf "Size of evaluated, unprocessed table: %i bytes\n" s
    printf
        "Size of raw strings (accumulated) %i bytes (%.2f%%)\n"
        s5
        (asPercent s5 s)
    strings `deepseq` return ()
    lsize <- recSize strings
    let ulist = map (const ()) strings
    ulist `deepseq` return ()
    lusize <- recSize ulist
    printf "Size of:\n    string list: %i bytes\n    list: %i bytes\n    difference: %i bytes\n" lsize lusize (lsize - lusize)
    serialized <- runKV $ serializeTable d
    forceLazyByteString serialized `seq` return ()
    s4 <- recSize serialized
    printf
        "Size of evaluated, serialized table: %i bytes (%.2f%%)\n"
        s4
        (asPercent s4 s)
    compressed <- flip evalStateT st $ encryptTable =<< compressTable serialized
    forceLazyByteString compressed `seq` return ()
    assertNF compressed -- probably useless because of how lazy bytestring implements NFData
    s1 <- recSize compressed
    printf
        "Size of evaluated, processed table: %i bytes (%.2f%%)\n"
        s1
        (asPercent s1 s)
    let strict = BS.toStrict compressed
    strict `deepseq` return ()
    assertNF strict
    s2 <- recSize strict
    printf
        "Size of evaluated, processed table as strict bytestring %i bytes (%.2f%%)\n"
        s2
        (asPercent s2 s)
    recreated <-
        flip evalStateT st $
        deserializeTable =<< decompressTable =<< decryptTable compressed
    recreated `deepseq` return ()
    s3 <- recSize recreated
    printf "Size of recreated table: %i bytes (%.2f%%)\n" s3 (asPercent s3 s)
    putStrLn $
        "Tables are " ++
        if recreated == d
            then "equal"
            else "unequa"
