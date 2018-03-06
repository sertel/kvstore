{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE ImplicitParams #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Microbenchmark where

import Test.HUnit hiding (State)
import Test.Framework
import Test.Framework.Providers.HUnit

import           System.Random
import           Control.Monad.State
import           Control.Lens

import qualified Data.HashMap.Strict       as HM
import qualified Data.HashSet              as Set
import           Data.List
import qualified Data.Vector               as V
import           Data.IORef

import           Kvservice_Types
import           Kvstore.KVSTypes

import           Requests
import           TestSetup
import           ServiceConfig

import           Debug.Trace

valueTemplate v = "value-" ++ show v
fieldTemplate f = "field-" ++ show f
keyTemplate k = "key-" ++ show k
tableTemplate t = "table-" ++ show t

data RangeGen = RangeGen Int Int StdGen
instance RandomGen RangeGen where
  next (RangeGen lo hi g) = let (i,g') = randomR (lo,hi) g in (i, RangeGen lo hi g')
  split (RangeGen lo hi g) = let (g1,g2) = split g in (RangeGen lo hi g1, RangeGen lo hi g2)

data ConstGen = ConstGen Int
instance RandomGen ConstGen where
  next (ConstGen i) = (i, ConstGen $ i+1)
  split (ConstGen i) = (ConstGen i, ConstGen i)

data BenchmarkState g = BenchmarkState {
                        _fieldCount :: Int,
                        _fieldSelection :: RangeGen,
                        _valueSizeGen :: RangeGen,
                        _tableCount :: Int,
                        _tableSelection :: RangeGen,
                        _keySelection :: g,
                        _operationSelection :: RangeGen,
                        _fieldCountSelection :: RangeGen,
                        _scanCountSelection :: RangeGen
                      }

makeLenses ''BenchmarkState
-- lens does not work with RankNTypes :(

createValue :: forall g. RandomGen g => StateT (BenchmarkState g) IO String
createValue = do
  bmState <- get
  let valSizeGenerator = view valueSizeGen bmState
      (size,valSizeGenerator') = next valSizeGenerator
  put $ over valueSizeGen (const valSizeGenerator') bmState
  (return . concatMap valueTemplate . take size) [1,2..]

createRequest :: forall g. RandomGen g => StateT (BenchmarkState g) IO KVRequest
createRequest = do
  bmState <- get
  let opSelector = view operationSelection bmState
      (op,opSelector') = next opSelector
      bmState' = over operationSelection (const opSelector') bmState

      tableSelector = view tableSelection bmState
      (tableId,tableSelector') = next tableSelector
      table = tableTemplate tableId
      bmState'' = over tableSelection (const tableSelector') bmState'

      keySelector = view keySelection bmState
      (keyId,keySelector') = next keySelector
      key = keyTemplate keyId
      bmState''' = over keySelection (const keySelector') bmState''
  put bmState'''
  case op of
        0 -> prepareINSERT table key <$> createINSERTEntry
        1 -> prepareUPDATE table key <$> createUPDATEEntry
        2 -> prepareREAD table key . Set.map fieldTemplate <$> getFields
        3 -> prepareSCAN table key <$> getScanCount <*> (Set.map fieldTemplate <$> getFields)
        4 -> return $ prepareDELETE table key
        _ -> error $ "No such operation: " ++ show op
    where
      getFieldsAndValues :: forall g. RandomGen g => [Int] -> StateT (BenchmarkState g) IO (HM.HashMap String String)
      getFieldsAndValues = fmap HM.fromList . mapM (\ i -> (,) <$> pure (fieldTemplate i) <*> createValue)
      createINSERTEntry :: forall g. RandomGen g => StateT (BenchmarkState g) IO (HM.HashMap String String)
      createINSERTEntry = getFieldsAndValues . flip take  [1,2..] . view fieldCount =<< get
      getFields :: forall g. RandomGen g => StateT (BenchmarkState g) IO (Set.HashSet Int)
      getFields = do
        s <- get
        let fieldCountGen = view fieldCountSelection s
            (fieldCount,fieldCountGen') = next fieldCountGen
            s' = over fieldCountSelection (const fieldCountGen') s
            fieldSel = view fieldSelection s'
            (fieldSel', fields) = foldl (\ (sel,l) _ -> let (f,sel') = next sel in (sel',l ++ [f]) )
                                       (fieldSel,[])
                                       $ take fieldCount [1,2..]
            s'' = over fieldSelection (const fieldSel') s'
        put s''
        return $ Set.fromList fields
      createUPDATEEntry :: forall g. RandomGen g => StateT (BenchmarkState g) IO (HM.HashMap String String)
      createUPDATEEntry = getFieldsAndValues . Set.toList =<< getFields
      getScanCount = do
        s <- get
        let scanCountSel = view scanCountSelection s
            (scanCount,scanCountSel') = next scanCountSel
        put $ over scanCountSelection (const scanCountSel') s
        return scanCount

workload :: forall g. RandomGen g => Int -> StateT (BenchmarkState g) IO (V.Vector KVRequest)
workload operationCount = V.fromList <$> mapM (const createRequest) [1..operationCount]

showState :: KVSState MockDB -> IO String
showState (KVSState cache dbRef _ _) = do
  db <- readIORef dbRef
  return $ "Cache:\n" ++ show cache ++ "\nDB:\n" ++ show db

runBatch :: (?execRequests :: ExecReqFn) => Assertion
runBatch =  do
  s <- initState
  -- fill the db first
  (requests,_) <- runStateT (workload 10) $ BenchmarkState
                                            10 -- _fieldCount
                                            (RangeGen 0 10 $ mkStdGen 0) -- _fieldSelection
                                            (RangeGen 5 10 $ mkStdGen 0) -- _valueSizeGen
                                            1 -- _tableCount
                                            (RangeGen 1 1 $ mkStdGen 0) -- _tableSelection
                                            (ConstGen 1) -- _keySelection
                                            (RangeGen 0 0 $ mkStdGen 0) -- _operationSelection (INSERT only)
                                            (RangeGen 3 10 $ mkStdGen 0) -- _fieldCountSelection
                                            (RangeGen 5 10 $ mkStdGen 0) -- _scanCountSelection
  traceM "requests (INSERT):"
  mapM (\i -> traceM $ show i ++ "\n" ) requests
  (_, s') <- flip runStateT s $ ?execRequests requests

  traceM "state after init:"
  traceM =<< showState s'

  -- then run some requests
  (requests,_) <- runStateT (workload 10) $ BenchmarkState
                                            10 -- _fieldCount
                                            (RangeGen 0 10 $ mkStdGen 0) -- _fieldSelection
                                            (RangeGen 5 10 $ mkStdGen 0) -- _valueSizeGen
                                            1 -- _tableCount
                                            (RangeGen 1 1 $ mkStdGen 0) -- _tableSelection
                                            (RangeGen 1 10 $ mkStdGen 0) -- _keySelection
                                            (RangeGen 1 3 $ mkStdGen 0) -- _operationSelection (no INSERT, no DELETE)
                                            (RangeGen 3 10 $ mkStdGen 0) -- _fieldCountSelection
                                            (RangeGen 5 10 $ mkStdGen 0) -- _scanCountSelection
  traceM $ "requests:"
  mapM (\i -> traceM $ show i ++ "\n" ) requests
  (responses, s'') <- flip runStateT s' $ ?execRequests requests
  traceM "???????????????????????????????????????"
  traceM $ "responses:"
  mapM (\i -> traceM $ show i ++ "\n" ) responses
  assertEqual "wrong number of responses" 100 $ length responses

suite :: (?execRequests :: ExecReqFn) => String -> [Test.Framework.Test]
suite name = [
               testCase "\n=======================================================" (return ())
             , testCase ("*** Running the " ++ name ++ " version: ***") (return ())
             , testCase "running batch" runBatch
             , testCase "=======================================================" (return ())
             ]
