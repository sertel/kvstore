
import Test.HUnit hiding (State)
import Test.Framework
import Test.Framework.Providers.HUnit

simpleTest :: Assertion
simpleTest = do
  -- let (result,state) = runOhuaM (simpleComposition 333 10) [0,0]
  -- assertEqual "result was wrong." 36 result
  -- assertEqual "state was wrong." [2,3] state
  return undefined

none-sense

main :: IO ()
main = defaultMainWithOpts
       [
        testCase "running some requests against the kvs" simpleTest
       ]
       mempty
