module LazyObject (LazyObject, read, write, new, newChanged, encodeObject, lazyO) where

import Data.ByteString.Lazy (ByteString)
import Control.DeepSeq
import Control.Lens (Lens)
import Data.Function (on)

import Prelude hiding (read)

type Encoded = ByteString
type Decoder a = Encoded -> a
type Encoder a = a -> Encoded

instance Show a  => Show (LazyObject a) where
  show = show . read

data LazyObject a
  = Unchanged Encoded a
  | Written a

instance NFData o => NFData (LazyObject o) where
  rnf = rnf . read

instance Monoid a => Monoid (LazyObject a) where
  mempty = newChanged mempty
  a `mappend` b = newChanged $ read a `mappend` read b

instance Eq a => Eq (LazyObject a) where
  (==) = (==) `on` read

read :: LazyObject a -> a
read (Unchanged _ t) = t
read (Written t) = t

new :: Decoder a -> Encoded -> LazyObject a
new f s = Unchanged s (f s)

newChanged :: a -> LazyObject a
newChanged = Written

write :: (a -> b) -> LazyObject a -> LazyObject b
write f t = newChanged $ f $ read t


encodeObject :: Encoder a -> LazyObject a -> ByteString
encodeObject _ ( Unchanged bs _ ) = bs
encodeObject g ( Written o ) = g o

lazyO :: Lens (LazyObject a) (LazyObject b) a b
lazyO f o = newChanged <$> f (read o)
