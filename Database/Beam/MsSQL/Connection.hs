{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE BangPatterns #-}

module Database.Beam.MsSQL.Connection
    ( MsSQL(..), MsSQLM

    , runBeamMsSQL, runBeamMsSQLDebug

    , MsSQLCommandSyntax(..)
    , MsSQLSelectSyntax(..), MsSQLInsertSyntax(..)
    , MsSQLUpdateSyntax(..), MsSQLDeleteSyntax(..)
    , MsSQLExpressionSyntax(..)
    ) where

import           Database.Beam
import           Database.Beam.Backend
import           Database.Beam.Query.SQL92

import           Database.Beam.MsSQL.FromField (FromField(..))
import           Database.Beam.MsSQL.Parser
import           Database.Beam.MsSQL.Syntax

import           Control.Applicative ((<|>))
import           Control.Exception (finally, throwIO)
import           Control.Monad.Free.Church
import           Control.Monad.Reader

import           Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Streaming as SBS
import           Data.Functor.Compose
import           Data.IORef
import           Data.Int (Int8, Int16, Int32, Int64)
import           Data.Monoid (First(..))
import           Data.Proxy
import           Data.Scientific
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import           Data.Text.Lazy.Builder
import           Data.Time
import           Data.Word (Word, Word8, Word16, Word32, Word64)
-- import           Debug.Trace

import qualified Database.TDS as TDS
import qualified Database.TDS.Proto as TDS.Proto

import           Streaming.Prelude hiding (show, length, mapM_, next)
import qualified Streaming as S

data MsSQL = MsSQL

instance BeamBackend MsSQL where
    type BackendFromField MsSQL = FromField

instance BeamSqlBackend MsSQL
type instance BeamSqlBackendSyntax MsSQL = MsSQLCommandSyntax

newtype MsSQLM a = MsSQLM (ReaderT (String -> IO (), TDS.Connection) IO a)
    deriving (Monad, MonadIO, Applicative, Functor)

runBeamMsSQLDebug :: (String -> IO ()) -> TDS.Connection -> MsSQLM a -> IO a
runBeamMsSQLDebug dbg conn (MsSQLM a) = runReaderT a (dbg, conn)

runBeamMsSQL :: TDS.Connection -> MsSQLM a -> IO a
runBeamMsSQL conn m = runBeamMsSQLDebug (\_ -> pure ()) conn m

instance MonadBeam MsSQL MsSQLM where
    runReturningMany (MsSQLCommandSyntax (MsSQLSyntax cmd)) consume =
        MsSQLM . ReaderT  $ \(dbg, tds) -> do
          let cmdTxt = TL.toStrict (toLazyText cmd)

              sqlBatch = TDS.Proto.mkPacket (TDS.Proto.mkPacketHeader TDS.Proto.SQLBatch mempty)
                                            cmdTxt

          dbg (T.unpack cmdTxt)

          getRes <- TDS.tdsSendPacket tds sqlBatch

          TDS.ResponseResultReceived (TDS.Proto.RowResults rowStream) <- getRes

          rowResult' <- S.inspect rowStream
          case rowResult' of
            Left () -> fail "No results returned"
            Right (Compose (rowMetadata :> rows)) -> do
              restStream <- newIORef (Right rows)

              let MsSQLM after = consume (liftIO nextRow')
                  nextRow' = nextRow rowMetadata restStream

              runReaderT after (dbg, tds)
                `finally` finishStream restStream nextRow'

type RowSource a
    = IORef (Either a (RawRowSource a))
type RawRowSource a
    = Stream (Stream TDS.Proto.RawColumn IO) IO a

nextRow :: forall x a. FromBackendRow MsSQL x
        => TDS.Proto.ColumnMetadata -> RowSource a -> IO (Maybe x)
nextRow (TDS.Proto.ColumnMetadata _ cols) src = do
  cur <- readIORef src
  case cur of
    Right _ | length cols /= valuesNeeded (Proxy @MsSQL) (Proxy @x) -> fail "Row length mismatch"
    Right curSrc -> do
       res <- parseMsSQLRow fromBackendRow curSrc
       case res of
         RowResultError x -> throwIO x -- TODO finish up sourcing
         RowResultComplete a -> writeIORef src (Left a) >> pure Nothing
         RowResultRow row nextSrc -> do
           writeIORef src (Right nextSrc)
           pure (Just row)

    _ -> pure Nothing

data RowResult x a
    = RowResultError !BeamRowReadError
    | RowResultComplete !x
    | RowResultRow !a !(RawRowSource x)

runPParserStreaming :: PParser a -> SBS.ByteString IO () -> IO (Maybe (a, SBS.ByteString IO ()))
runPParserStreaming (PParser len onEof process) bs = do
  let chunkStr = SBS.splitAt (fromIntegral len) bs
  chunk :> bs' <- SBS.toLazy chunkStr

  if fromIntegral (BL.length chunk) == len
     then process chunk $ \case
            PParseSuccess a -> pure (Just (a, bs'))
            PParseFails -> pure Nothing
            PParseMore next -> runPParserStreaming next bs'
     else let complete = \case
                PParseSuccess a -> pure (Just (a, mapM_ SBS.chunk (BL.toChunks chunk) >> bs'))
                PParseMore (PParser 0 onDone' _) ->
                    onDone' complete
                _ -> pure Nothing
          in onEof complete

newtype ParseRowStep x = ParseRowStep (forall r. (Either (Either BeamRowReadError x) (TDS.Proto.ColumnData -> PParser (ParseRowStep x)) -> r) -> r)

andThen :: ParseRowStep a1 -> (a1 -> ParseRowStep x1) -> ParseRowStep x1
andThen (ParseRowStep first) mkThen =
    ParseRowStep $ \cont ->
        first $ \case
          Left (Left e) -> cont (Left (Left e))
          Left (Right x) ->
              case mkThen x of
                ParseRowStep then' -> then' cont
          Right x -> cont (Right (\proto -> fmap (`andThen` mkThen) (x proto)))

parseMsSQLRow :: FromBackendRowM MsSQL x -> RawRowSource a -> IO (RowResult a x)
parseMsSQLRow (FromBackendRowM rowParser) rowSrc =
  S.inspect rowSrc >>= \case
   Left x -> pure (RowResultComplete x)
   Right columnSrc -> do
     case runF rowParser finish step of
       ParseRowStep go -> go (parseColumns 0 columnSrc)

  where
    parseColumns :: Int -> Stream TDS.Proto.RawColumn IO (RawRowSource a)
                 -> Either (Either BeamRowReadError x) (TDS.Proto.ColumnData -> PParser (ParseRowStep x))
                 -> IO (RowResult a x)
    parseColumns !colIndex src (Left (Right x)) = do
      -- Continue with src
      r <- S.inspect src
      case r of
        Left next ->
            pure (RowResultRow x next)
        Right (TDS.Proto.RawColumn _ _ _) ->
            pure (RowResultError (BeamRowReadError (Just colIndex) (ColumnErrorInternal "There were more columns available than we could parse")))

    parseColumns _ _ (Left (Left e)) = do
      pure (RowResultError e)

    parseColumns !colIndex src (Right ps) = do
      x <- S.inspect src
      case x of
        Left {} ->
            pure (RowResultError (BeamRowReadError (Just colIndex) (ColumnNotEnoughColumns 0)))
        Right (TDS.Proto.RawColumn colData s nextColSrc) -> do
          -- Parse y with ps
          res <- runPParserStreaming (ps colData) s
          case res of
            Nothing -> pure (RowResultError (BeamRowReadError (Just colIndex) (ColumnTypeMismatch "unknown" "unknown" "parse failed")))
            Just (ParseRowStep next', remaining) ->
                next' (parseColumns (colIndex + 1) (nextColSrc remaining))

    finish :: forall x. x -> ParseRowStep x
    finish ret = ParseRowStep (\cont -> cont (Left (Right ret)))


    step :: forall x. FromBackendRowF MsSQL (ParseRowStep x) -> ParseRowStep x
    step (ParseOneField next) =
        ParseRowStep (\cont ->
                          cont (Right (\column -> do
                                         parsed <- parseMsSQLColumn column
                                         pure (next parsed))))

    step (Alt (FromBackendRowM a) (FromBackendRowM b) next) =
        let recurs (ParseRowStep aGo) (ParseRowStep bGo) =
                aGo (\case
                      Left (Left done) -> ParseRowStep (\cont -> cont (Left (Left done)))
                      Left (Right x) -> next x
                      Right aOnParse ->
                          bGo (\case
                                Left (Left done) -> ParseRowStep (\cont -> cont (Left (Left done)))
                                Left (Right x) -> next x
                                Right bOnParse ->
                                    ParseRowStep (\cont -> cont (Right (\colData -> do
                                                                          (First aRes, First bRes) <-
                                                                              simultaneous (aOnParse colData >>= \x -> pure (First (Just x), mempty))
                                                                                           (bOnParse colData >>= \x -> pure (mempty, First (Just x)))
                                                                          case (aRes, bRes) of
                                                                            (Nothing, Nothing) -> fail "No parse"
                                                                            (Just a', Nothing) -> pure (a' `andThen` next)
                                                                            (Nothing, Just b') -> pure (b' `andThen` next)
                                                                            (Just a', Just b') -> pure (recurs a' b'))))))
        in recurs (runF a finish step) (runF b finish step)

    step (FailParseWith err) =
        ParseRowStep (\cont -> cont (Left (Left err)))

finishStream :: RowSource (Stream f IO r) -> IO (Maybe a) -> IO ()
finishStream src next =
    do res <- next
       case res of
         Nothing -> do
           Left x <- readIORef src
           res' <- S.inspect x
           case res' of
             Left {} -> pure ()
             Right {} -> fail "More results after main result"
         Just {} -> finishStream src next

-- * 'FromBackendRow' instances

instance FromBackendRow MsSQL Int16
instance FromBackendRow MsSQL Int32
instance FromBackendRow MsSQL Int64
instance FromBackendRow MsSQL Int where
    fromBackendRow = (fromIntegral <$> fromBackendRow @MsSQL @Int16) <|>
                     (fromIntegral <$> fromBackendRow @MsSQL @Int32) <|>
                     (fromIntegral <$> fromBackendRow @MsSQL @Int64)
instance FromBackendRow MsSQL T.Text
instance FromBackendRow MsSQL SqlNull
instance FromBackendRow MsSQL LocalTime
instance FromBackendRow MsSQL Scientific

instance BeamSqlBackendIsString MsSQL String
instance BeamSqlBackendIsString MsSQL T.Text

#define MS_HAS_EQUALITY_CHECK(ty)                   \
 instance HasSqlEqualityCheck MsSQL (ty);           \
 instance HasSqlQuantifiedEqualityCheck MsSQL (ty);
MS_HAS_EQUALITY_CHECK(Bool)
MS_HAS_EQUALITY_CHECK(Double)
MS_HAS_EQUALITY_CHECK(Float)
MS_HAS_EQUALITY_CHECK(Int)
MS_HAS_EQUALITY_CHECK(Int8)
MS_HAS_EQUALITY_CHECK(Int16)
MS_HAS_EQUALITY_CHECK(Int32)
MS_HAS_EQUALITY_CHECK(Int64)
MS_HAS_EQUALITY_CHECK(Integer)
MS_HAS_EQUALITY_CHECK(Word)
MS_HAS_EQUALITY_CHECK(Word8)
MS_HAS_EQUALITY_CHECK(Word16)
MS_HAS_EQUALITY_CHECK(Word32)
MS_HAS_EQUALITY_CHECK(Word64)
MS_HAS_EQUALITY_CHECK(T.Text)
MS_HAS_EQUALITY_CHECK(TL.Text)
MS_HAS_EQUALITY_CHECK(UTCTime)
MS_HAS_EQUALITY_CHECK(LocalTime)
MS_HAS_EQUALITY_CHECK(ZonedTime)
MS_HAS_EQUALITY_CHECK(TimeOfDay)
MS_HAS_EQUALITY_CHECK(NominalDiffTime)
MS_HAS_EQUALITY_CHECK(Day)
MS_HAS_EQUALITY_CHECK([Char])
MS_HAS_EQUALITY_CHECK(Scientific)
MS_HAS_EQUALITY_CHECK(ByteString)
MS_HAS_EQUALITY_CHECK(BL.ByteString)

instance HasQBuilder MsSQL where
    buildSqlQuery = buildSql92Query' True

