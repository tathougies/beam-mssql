module Database.Beam.MsSQL
    ( module Database.Beam.MsSQL.Connection
    , withTransaction
    ) where

import Database.Beam.MsSQL.Connection

import Database.TDS ( withTransaction )
