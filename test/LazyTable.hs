module LazyTable (LazyTable, read, write, new, newChanged, lazyDecode) where

import Data.ByteString.Lazy (ByteString)
import Data.Binary

import Prelude hiding (read)

type Encoded = ByteString
type Decoder a = Encoded -> a

data LazyTable a
  = Unchanged Encoded a
  | Written a

read :: LazyTable a -> a
read (Unchanged _ t) = t
read (Written t) = t

new :: Decoder a -> Encoded -> LazyTable a
new f s = Unchanged s (f s)

newChanged :: a -> LazyTable a
newChanged = Written

write :: (a -> b) -> LazyTable a -> LazyTable b
write f t = Written $ f $ read t

lazyDecode :: Binary a => ByteString -> LazyTable a
lazyDecode = new decode
