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
    , batchSize = s
    , useEncryption = True
    , numTables = 20
    , threadCount = c
    , systemVersion = Ohua_SBFM
    }
  | c <- [1,2,4]
  , s <- [30,60,120,240]
  ]

runExp c = do
  printf "Running %s on %d cores with %d requests per batch... " (show $ systemVersion c) (threadCount c) (batchSize c)
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
