{-# LANGUAGE TupleSections, CPP #-}
module LazyObject
    ( LazyObject
    , read
    , write
    , new
    , newChanged
    , encodeObject
    , Encoder
    , Decoder
    , Encoded
    , lazyO
    , printLazySerUsage
    , enableStatCollection
    ) where

import Data.ByteString.Lazy (ByteString)
import Control.DeepSeq
import Control.Lens (Lens)
import Data.Function (on)

import Prelude hiding (read)


import Data.IORef
import System.IO.Unsafe
import Text.Printf
import System.IO (stderr, hPutStrLn)

type Encoded = ByteString
type Decoder a = Encoded -> a
type Encoder a = a -> Encoded

instance Show a  => Show (LazyObject a) where
  show = show . read

data LazyObject a
  = Unchanged Encoded a
  | Written a

instance NFData o => NFData (LazyObject o) where
  rnf (Written a) = rnf a
  rnf _ = ()

instance Monoid a => Monoid (LazyObject a) where
  mempty = newChanged mempty

#if MIN_VERSION_base(4,11,0)
instance Semigroup a => Semigroup (LazyObject a) where
  a <> b = newChanged $ read a <> read b
#else
  a `mappend` b = newChanged $ read a `mappend` read b
#endif

instance Eq a => Eq (LazyObject a) where
  (==) = (==) `on` read

collectStatsRef = unsafePerformIO $ newIORef False
{-# NOINLINE collectStatsRef #-}

collectStats = unsafePerformIO $ readIORef collectStatsRef

enableStatCollection = writeIORef collectStatsRef True

evaluations = unsafePerformIO $ newIORef (0 :: Word)
lazySerializes = unsafePerformIO $ newIORef (0 :: Word)
reportEvaluation
    | collectStats =
        \o ->
            unsafePerformIO $ do
                atomicModifyIORef evaluations $ (, ()) . succ
                return o
    | otherwise = id

reportSerializtion
    | collectStats =
        \w ->
            unsafePerformIO $ do
                atomicModifyIORef lazySerializes $ (, ()) . succ
                return w
    | otherwise = id

printLazySerUsage :: IO ()
printLazySerUsage
    | collectStats = do
        evals <- readIORef evaluations
        lazies <- readIORef lazySerializes
        hPrintf
            stderr
            "%d evaluations and %d lazy serializations\n"
            evals
            lazies
    | otherwise = hPutStrLn stderr "Stat collection not enabled (or too late)."

read :: LazyObject a -> a
read (Unchanged _ t) = t
read (Written t) = t

new :: Decoder a -> Encoded -> LazyObject a
new f s = Unchanged s (reportEvaluation $ f s)

newChanged :: a -> LazyObject a
newChanged = Written

write :: (a -> b) -> LazyObject a -> LazyObject b
write f t = newChanged $ f $ read t


encodeObject :: Encoder a -> LazyObject a -> ByteString
encodeObject _ ( Unchanged bs _ ) = reportSerializtion bs
encodeObject g ( Written o ) = g o

lazyO :: Lens (LazyObject a) (LazyObject b) a b
lazyO f o = newChanged <$> f (read o)
