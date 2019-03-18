{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}

module Main where

import Chinook.Schema

import Database.Beam
import Database.Beam.MsSQL
import Database.TDS

import Control.Monad

import Data.Monoid

import GHC.IO.Encoding

main :: IO ()
main = do
  setLocaleEncoding utf8

  let o = tdsOptionHost "localhost" <> tdsOptionPort 1433 <>
          tdsDebugLogging <> tdsOptionUserAndPassword "sa" "Testtest1" <>
          tdsOptionDatabase "test2"

  conn <- login o

  putStrLn "Going to run query"
--  res <- withTransaction conn $
--         runBeamMsSQLDebug putStrLn conn $
--         runSelectReturningList (select (all_ (album chinookDb)))
--
--  forM_ res $ \row ->
--      putStrLn ("Album: " ++ show row)
--
--  res <- runBeamMsSQLDebug putStrLn conn $
--         runSelectReturningList $ select $ do
--           e <- all_ (employee chinookDb)
--           mgr <- leftJoin_' (all_ (employee chinookDb)) (\mgr -> employeeReportsTo e ==?. just_ (pk mgr))
--           pure (e, mgr)
--
--  forM_ res $ \row ->
--      putStrLn ("Employee mgr: " ++ show row)

--   res <- runBeamMsSQLDebug putStrLn conn $
--           runSelectReturningList $ select $
--           let tracksLongerThanThreeMinutes =
--                   fmap trackId $
--                   filter_ (\t -> trackMilliseconds t >=. 180000) $
--                   all_ (track chinookDb)
--           in filter_ (\ln -> let TrackId lnTrackId = invoiceLineTrack ln
--                              in unknownAs_ False (lnTrackId ==*. anyOf_ tracksLongerThanThreeMinutes)) $
--              all_ (invoiceLine chinookDb)
-- 
-- 
--   forM_ res $ \tk ->
--      putStrLn ("Track: " ++ show tk)

  res <- runBeamMsSQLDebug putStrLn conn $
         runSelectReturningList $ select $
--         nub_ $ fmap (\a -> ( customerFirstName a, customerAddress $ a)) $
         all_ (customer chinookDb)


  forM_ res $ \pc ->
      putStrLn ("Postal code: " ++ show pc)



--res <- runBeamMsSQLDebug putStrLn conn $
--       runSelectReturningList $ select $
--       do i <- all_ (invoice chinookDb)
--          ln <- all_ (invoiceLine chinookDb)
--          guard_ (invoiceLineInvoice ln `references_` i)
--          pure (i, ln)

  --forM_ res $ \row ->
  --    putStrLn ("Album: " ++ show row)
