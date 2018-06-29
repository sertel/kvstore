#!stack runhaskell
{-# LANGUAGE OverloadedStrings #-}
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
  [ BatchConfig
    { keyCount = 100
    , batchCount = 30
    , batchSize = 30
    , useEncryption = True
    , numTables = 100
    , threadCount = c
    , systemVersion = v
    }
  | c <- [1..8]
  , v <- [Functional, Ohua_FBM, Ohua_SBFM]
  ]

runExp c = do
  printf "Running %s on %d cores... " (show $ systemVersion c) (threadCount c)
  (code, res, err) <- readProcessWithExitCode "stack" ["exec", "--", "microbench", "+RTS", "-N" ++ show (threadCount c)] (encode c)
  hFlush stdout
  unless (code == ExitSuccess) $ do
    putStrLn "Failed the run"
    B.putStrLn err
    error "failed run"
  let d = either error id $ eitherDecode res :: Double
  printf "%f\n" d
  return $ object
    [ "data" .= d
    , "config" .= c
    ]

main = B.writeFile "results.json" . encode =<< mapM runExp confs
