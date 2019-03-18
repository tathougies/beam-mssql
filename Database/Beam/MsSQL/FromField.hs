{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}

module Database.Beam.MsSQL.FromField where

import           Database.Beam.Backend.SQL.Types
import           Database.Beam.MsSQL.Parser

import           Control.Monad.Except

import qualified Data.Binary as Bin
import qualified Data.Binary.Get as Bin
import           Data.Bits
import qualified Data.ByteString.Lazy as BL
import           Data.Int
import           Data.Ratio
import           Data.Scientific
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import           Data.Time
-- import           Data.Time.LocalTime
import           Data.Typeable (Typeable)
import           Data.Word

-- import           Debug.Trace

import qualified Database.TDS.Proto as TDS.Proto

data MsSQLMarshalError
    = MsSQLUnexpectedNull
    | MsSQLTypeMismatchError String
    | MsSQLUserError String
      deriving Show

class Typeable a => FromField a where
    parseMsSQLColumn :: TDS.Proto.ColumnData
                     -> PParser a

typeMismatchError :: MonadError MsSQLMarshalError m => String -> m a
typeMismatchError = throwError . MsSQLTypeMismatchError

nullError :: MonadError MsSQLMarshalError m => m a
nullError = throwError MsSQLUnexpectedNull

-- parseError errs = MsSQLUserError ("Parse error: " ++ show errs)
-- 
parseColumn :: Show a
            => (TDS.Proto.TypeInfo -> ExceptT MsSQLMarshalError PParser a)
            -> TDS.Proto.ColumnData -> PParser a
parseColumn parse colData =
  do res <- runExceptT (parse (TDS.Proto.cdBaseTypeInfo colData))
     case res of
       Left e -> fail ("Left parse err: " ++ show e)
       Right x -> pure x
-- 
-- instance FromField Int16 where
--     parseMsSQLColumn =
--       parseColumn $ \case
--         TDS.Proto.IntNType False 2 ->
--           fromIntegral <$> lift anyWord16le
--         TDS.Proto.IntNType {} -> typeMismatchError "Invalid int type"
--         _ -> typeMismatchError "Type mismatch"

instance FromField Int16 where
    parseMsSQLColumn =
        parseColumn $ \case
          TDS.Proto.IntNType vl 2 -> do
              handleLenNonNull vl
              Bin.runGet Bin.getInt16le <$> lift (satisfy 2 (const True))
          TDS.Proto.IntNType {} ->
              typeMismatchError "Invalid int type"
          _ -> typeMismatchError "Type mismatch"

instance FromField Int32 where
    parseMsSQLColumn =
        parseColumn $ \case
          TDS.Proto.IntNType vl 4 -> do
              handleLenNonNull vl
              Bin.runGet Bin.getInt32le <$> lift (satisfy 4 (const True))
          TDS.Proto.IntNType {} ->
              typeMismatchError "Invalid int type"
          _ -> typeMismatchError "Type mismatch"

instance FromField Int64 where
    parseMsSQLColumn =
        parseColumn $ \case
          TDS.Proto.IntNType vl 8 -> do
              handleLenNonNull vl
              Bin.runGet Bin.getInt64le <$> lift (satisfy 8 (const True))
          _ -> typeMismatchError "Type mismatch"

instance FromField T.Text where
    parseMsSQLColumn =
        parseColumn $ \case
          TDS.Proto.VarcharType typeLen TDS.Proto.NationalChar colLen _
             | colLen == 0xFFFF -> nullError
             | otherwise -> do
                 len <- case typeLen of
                          TDS.Proto.ByteLen -> do
                            len :: Word8 <- Bin.decode <$> lift (satisfy 1 (const True))
                            when (len == 0xFF) nullError
                            pure (fromIntegral len)
                          TDS.Proto.ShortLen -> do
                            len :: Word16 <- Bin.runGet Bin.getWord16le <$> lift (satisfy 2 (const True))
                            when (len == 0xFFFF) nullError
                            pure (fromIntegral len)
                 d <- lift (satisfy len (const True))
                 pure (T.decodeUtf16LE (BL.toStrict d))
          ty -> typeMismatchError "Type mismatch"

instance FromField SqlNull where
    parseMsSQLColumn colData =
        case TDS.Proto.cdBaseTypeInfo colData of
          TDS.Proto.NullType -> pure SqlNull
          TDS.Proto.IntNType True _ -> parseNullByte
          TDS.Proto.DecimalType True _ _ -> parseNullByte
          TDS.Proto.NumericType True _ _ -> parseNullByte
          TDS.Proto.BitNType True _ -> parseNullByte
          TDS.Proto.DecimalNType True _ _ -> parseNullByte
          TDS.Proto.NumericNType True _ _ -> parseNullByte
          TDS.Proto.FloatNType True _ -> parseNullByte
          TDS.Proto.MoneyNType True _ -> parseNullByte
          TDS.Proto.DtTmNType True _ -> parseNullByte
          TDS.Proto.DateNType True _ -> parseNullByte
          TDS.Proto.TimeNType True _ -> parseNullByte
          TDS.Proto.CharType len _ _ _ -> parseNullChar len
          TDS.Proto.VarcharType len _ _ _ -> parseNullChar len
          _ -> fail "not null"

        where
          parseNullByte =
            SqlNull <$ satisfy 1 (\d -> Bin.decode d == (0x0 :: Word8))

          parseNullChar TDS.Proto.ByteLen =
            SqlNull <$ satisfy 1 (\d -> d == BL.pack [ 0xFF ])
          parseNullChar TDS.Proto.ShortLen =
              SqlNull <$ satisfy 2 (\d -> d == BL.pack [ 0xFF, 0xFF ])
-- take16LE :: Monad m => SBS.ByteString m ()
--          -> ExceptT MsSQLMarshalError m (Word16, SBS.ByteString m ())
-- take16LE s = do
--   (lo, s')  <- SBS.uncons s  !? MsSQLUserError "Not enough input"
--   (hi, s'') <- SBS.uncons s' !? MsSQLUserError "Not enough input"
--   pure (fromIntegral hi `shiftL` 8 .|. fromIntegral lo, s'')
-- 
-- instance FromField T.Text where
--     parseMsSQLColumn colData s =
--       runExceptT $
--       case TDS.Proto.cdBaseTypeInfo colData of
--         TDS.Proto.VarcharType typeLen TDS.Proto.NationalChar colLen coll -> do
--           if colLen == 0xFFFF
--              then nullError
--              else do
--                (len, s') <-
--                   case typeLen of
--                     TDS.Proto.ByteLen -> first fromIntegral <$> -- (first fromIntegral) <$>
--                                          (SBS.uncons s !? MsSQLUserError "not enough input")
--                     TDS.Proto.ShortLen -> take16LE s
--                let s'' = SBS.splitAt (fromIntegral len) s'
--                bytesData S.:> s''' <- lift (SBS.toStrict s'')
--                pure (T.decodeUtf16LE bytesData, s''')
--         _ -> typeMismatchError "TEXT"
--

instance FromField LocalTime where
    parseMsSQLColumn =
        parseColumn $ \case
          TDS.Proto.DtTmNType vl 8 ->
            do handleLenNonNull vl
               daysSince01011900 <- Bin.runGet Bin.getWord32le <$>
                                    lift (satisfy 4 (const True))
               deltasSinceMidnight <- Bin.runGet Bin.getWord32le <$>
                                      lift (satisfy 4 (const True))

               let (secondsSinceMidnight, deltasAfterSecond) = deltasSinceMidnight `divMod` 300
                   secondsSinceMidnightF = fromIntegral secondsSinceMidnight + fromIntegral deltasAfterSecond / 300.0

                   epochDay = fromGregorian 1900 1 1
                   resDay = fromIntegral daysSince01011900 `addDays` epochDay

                   resUtc = UTCTime resDay secondsSinceMidnightF
               pure (utcToLocalTime utc resUtc)
          _ -> typeMismatchError "TIMESTAMP"

handleLenNonNull :: Bool -> ExceptT MsSQLMarshalError PParser ()
handleLenNonNull False = pure ()
handleLenNonNull True =
  () <$ lift (satisfy 1 (\d -> d /= BL.pack [ 0 ]))

-- instance FromField LocalTime where
--     parseMsSQLColumn =
--       parseColumn $ \case
--         TDS.Proto.DtTmNType _ 8 ->
--           do daysSince01011900   <- lift anyWord32le
--              deltasSinceMidnight <- lift anyWord32le
-- 
--              let (secondsSinceMidnight, deltasAfterSecond) = deltasSinceMidnight `divMod` 300
--                  secondsSinceMidnightF = fromIntegral secondsSinceMidnight + fromIntegral deltasAfterSecond / 300.0
-- 
--                  epochDay = fromGregorian 1900 1 1
-- 
--                  resDay = fromIntegral daysSince01011900 `addDays` epochDay
-- 
--                  resUtc = UTCTime resDay secondsSinceMidnightF
-- 
--              pure (utcToLocalTime utc resUtc)
--         _ -> typeMismatchError "TIMESTAMP"
-- 
instance FromField Scientific where
    parseMsSQLColumn =
      parseColumn $ \case
        TDS.Proto.IntNType False sz ->
          fromIntegral <$> lift (readIntSz sz)
        TDS.Proto.IntNType True sz -> do
          sz <- lift getLen8
          when (sz == 0) nullError
          fromIntegral <$> lift (readIntSz sz)
        TDS.Proto.DecimalNType nullable sz precScale ->
          parseNumeric "DECIMAL" nullable sz precScale
        TDS.Proto.NumericNType nullable sz precScale ->
          parseNumeric "NUMERIC" nullable sz precScale
        _ -> typeMismatchError "Scientific"

getLen8 :: PParser Word8
getLen8 = getWord8

getWord8 :: PParser Word8
getWord8 = Bin.runGet Bin.getWord8 <$> satisfy 1 (const True)

readIntSz :: Word8 -> PParser Integer
readIntSz 1 = fromIntegral <$> readInt8
readIntSz 2 = fromIntegral <$> readInt16
readIntSz 4 = fromIntegral <$> readInt32
readIntSz 8 = fromIntegral <$> readInt64
readIntSz n = do
  rawData <- satisfy (fromIntegral n) (const True)
  twosComplement n <$> readIntNLE rawData 0 0

twosComplement :: Word8 -> Integer -> Integer
twosComplement n i =
  if testBit i (fromIntegral n * 8 - 1)
  then negate (complement i + 1)
  else i

readInt8 :: PParser Int8
readInt8 = Bin.runGet Bin.getInt8 <$> satisfy 1 (const True)

readInt16 :: PParser Int16
readInt16 = Bin.runGet Bin.getInt16le <$> satisfy 2 (const True)

readInt32 :: PParser Int32
readInt32 = Bin.runGet Bin.getInt32le <$> satisfy 4 (const True)

readInt64 :: PParser Int64
readInt64 = Bin.runGet Bin.getInt64le <$> satisfy 8 (const True)

readIntNLE :: BL.ByteString -> Int64 -> Integer -> PParser Integer
readIntNLE d !n !i =
  case BL.uncons d of
    Nothing -> pure i
    Just (digit, d') ->
      readIntNLE d' (n + 1) (fromIntegral digit `shiftL` fromIntegral (n * 8))

-- instance FromField a => FromField (Maybe a) where
--     parseMsSQLColumn colData s =
--         fail ("Maybe a " ++ show colData)

parseNumeric :: String -> Bool -> Word8 -> TDS.Proto.PrecScale
             -> ExceptT MsSQLMarshalError PParser Scientific
parseNumeric tyName True sz precScale = do
  realSz <- lift getLen8
  when (realSz == 0) nullError
  parseNumeric tyName False realSz precScale

parseNumeric _ False _ (TDS.Proto.PrecScale p scale) = do
  sign <- lift getWord8
  let intSz | p <= 9 = 4
            | p <= 19 = 8
            | p <= 28 = 12
            | otherwise = 16

  num <- lift (readIntSz intSz)

  let res = num % (10 ^ fromIntegral scale :: Integer)

      res' :: Rational
      res' = if sign == 0 then negate res else res

  pure (fromRational res')
