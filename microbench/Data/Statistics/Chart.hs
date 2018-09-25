{-# LANGUAGE OverloadedLabels, TypeOperators, DataKinds,
  ViewPatterns, RankNTypes, FlexibleContexts #-}
module Data.Statistics.Chart where

import Control.Arrow
import Data.Statistics
import Named
import Ohua.Types
import Ohua.LensClasses
import Chart
import Chart.Bar
import Data.Semigroup
import Data.List (sortOn)
import qualified Data.Text as T
import Data.Ord


import Control.Lens
import Data.Generics.Labels()
-- import qualified Diagrams.Prelude as D

plotStats ::
     "showSystemOperators" :? Bool
  -> "numBars" :? Word
  -> Stats
  -> Chart b
plotStats
    (argDef #showSystemOperators False -> showSysOps)
    (argDef #numBars 20 -> numBars)
    stats =
  barChart def (BarData [barData] Nothing Nothing) <>
  hud
  ( #titles .~ [(def,"Operator Runtimes")] $
    #axes .~
    [ #tickStyle .~
      TickLabels barLabels $
      def
    ] $
    -- #range .~ Just (fold (abs <$> rs)) $
    def)
  where
    rs = rectBars 0.1 barData
    (barData, opNames) = unzip $ take ( fromIntegral numBars ) $ sortOn (Down . snd) $ map ((realToFrac . totalRuntime) &&& operatorName) vals
    barLabels = map (T.pack . show) opNames

    vals | showSysOps = filter ((/= "system") . view namespace . operatorName) stats
         | otherwise = stats
