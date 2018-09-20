#!stack runhaskell
{-# LANGUAGE OverloadedStrings, RecordWildCards #-}
import MBConfig
import Data.Aeson
import qualified Data.ByteString.Lazy.Char8 as B
import System.Process.ByteString.Lazy
import Text.Printf
import System.IO
import System.Exit
import Versions
import Control.Monad


confs =
    [ def
        { keyCount = 70
        , batchCount = 5
        , batchSize = 70
        , useEncryption = True
        , numTables = 100
        , numFields = 70
        , preloadCache = False
        , threadCount = c
        , systemVersion = v
        , readDelay = 0 --800000
        , writeDelay = 0 --1000000
        , requestSelection = Nothing
        , lazySerialization = lazy
        }
    | lazy <- [True, False]
    , c <- [1, 2, 4]
    , v <- [Functional, Ohua_SBFM, Ohua_FBM]
    ]

runExp c@BatchConfig {..} = do
    printf "Running %s on %d cores, lazy = %v ... " (show systemVersion) threadCount (show lazySerialization)
    (code, res, err) <-
        readProcessWithExitCode
            "stack"
            ["exec", "--", "microbench", "batches", "+RTS", "-A64m", "-n4m", "-N" ++ show threadCount]
            (encode c)
    hFlush stdout
    unless (code == ExitSuccess) $ do
        putStrLn "Failed the run"
        B.putStrLn err
        error "failed run"
    let d = either error id $ eitherDecode res :: Double
    printf "%f\n" d
    return $ object ["data" .= d, "config" .= c]

main = B.writeFile "results.json" . encode =<< mapM runExp confs
