{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-missing-fields #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}
{-# OPTIONS_GHC -fno-warn-name-shadowing #-}
{-# OPTIONS_GHC -fno-warn-unused-imports #-}
{-# OPTIONS_GHC -fno-warn-unused-matches #-}

-----------------------------------------------------------------
-- Autogenerated by Thrift Compiler (0.9.3)                      --
--                                                             --
-- DO NOT EDIT UNLESS YOU ARE SURE YOU KNOW WHAT YOU ARE DOING --
-----------------------------------------------------------------

module DB_Client(get,put) where
import qualified Data.IORef as R
import Prelude (($), (.), (>>=), (==), (++))
import qualified Prelude as P
import qualified Control.Exception as X
import qualified Control.Monad as M ( liftM, ap, when )
import Data.Functor ( (<$>) )
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Hashable as H
import qualified Data.Int as I
import qualified Data.Maybe as M (catMaybes)
import qualified Data.Text.Lazy.Encoding as E ( decodeUtf8, encodeUtf8 )
import qualified Data.Text.Lazy as LT
import qualified GHC.Generics as G (Generic)
import qualified Data.Typeable as TY ( Typeable )
import qualified Data.HashMap.Strict as Map
import qualified Data.HashSet as Set
import qualified Data.Vector as Vector
import qualified Test.QuickCheck.Arbitrary as QC ( Arbitrary(..) )
import qualified Test.QuickCheck as QC ( elements )

import qualified Thrift as T
import qualified Thrift.Types as T
import qualified Thrift.Arbitraries as T


import Db_Types
import DB
seqid = R.newIORef 0
get (ip,op) arg_key = do
  send_get op arg_key
  recv_get ip
send_get op arg_key = do
  seq <- seqid
  seqn <- R.readIORef seq
  T.writeMessageBegin op ("get", T.M_CALL, seqn)
  write_Get_args op (Get_args{get_args_key=arg_key})
  T.writeMessageEnd op
  T.tFlush (T.getTransport op)
recv_get ip = do
  (fname, mtype, rseqid) <- T.readMessageBegin ip
  M.when (mtype == T.M_EXCEPTION) $ do { exn <- T.readAppExn ip ; T.readMessageEnd ip ; X.throw exn }
  res <- read_Get_result ip
  T.readMessageEnd ip
  P.return $ get_result_success res
put (ip,op) arg_key arg_value = do
  send_put op arg_key arg_value
  recv_put ip
send_put op arg_key arg_value = do
  seq <- seqid
  seqn <- R.readIORef seq
  T.writeMessageBegin op ("put", T.M_CALL, seqn)
  write_Put_args op (Put_args{put_args_key=arg_key,put_args_value=arg_value})
  T.writeMessageEnd op
  T.tFlush (T.getTransport op)
recv_put ip = do
  (fname, mtype, rseqid) <- T.readMessageBegin ip
  M.when (mtype == T.M_EXCEPTION) $ do { exn <- T.readAppExn ip ; T.readMessageEnd ip ; X.throw exn }
  res <- read_Put_result ip
  T.readMessageEnd ip
  P.return $ put_result_success res
