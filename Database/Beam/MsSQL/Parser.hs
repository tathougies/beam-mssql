{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Defines a parser that can perform multiple parses over a
-- streaming bytestring in parallel. Parses that fail are removed from
-- consideration, while the first parse to succeed is the result of
-- the overall computation. Parses that succeed at the same time are
-- combined using the semigroup operator.

module Database.Beam.MsSQL.Parser where

import           Control.Monad.Fail

import qualified Data.ByteString.Lazy as BL
import           Data.Monoid
import           Data.Word (Word32)

-- import           Debug.Trace

data PParseResult a
    = PParseSuccess a
    | PParseFails
    | PParseMore (PParser a)
      deriving Functor

data PParser a
    = PParser
    { ppLenRequired :: !Word32
    , ppProcessEof  :: forall r. (PParseResult a -> r) -> r
    , ppProcess     :: forall r. BL.ByteString -> (PParseResult a -> r) -> r
    }

instance Functor PParser where
    fmap f ps = ps { ppProcessEof = \next -> ppProcessEof ps (next . fmap f)
                   , ppProcess = \d next -> ppProcess ps d (next . fmap f) }

instance Applicative PParser where
    pure (x :: a) =
        PParser 0 doRet (\_ -> doRet)
        where
          doRet :: forall r. (PParseResult a -> r) -> r
          doRet next = next (PParseSuccess x)
    f <*> x = do
      f' <- f
      x' <- x
      pure (f' x')

instance Monad PParser where
    return = pure
--    PParser 0 _ a >>= mkB =
--        a "" $ \case
--          PParseFails -> Prelude.fail ""
--          PParseMore a -> a >>= mkB
--          PParseSuccess x -> mkB x
    PParser aMin onDone a >>= mkB =
        PParser aMin
                (\next ->
                     onDone $ \case
                       PParseFails -> next PParseFails
                       PParseMore a' -> next (PParseMore (a' >>= mkB))
                       PParseSuccess x -> next (PParseMore (mkB x)))
                (\d next ->
                     a d $ \case
                       PParseSuccess x ->
                           next (PParseMore (mkB x))
                       PParseFails -> next PParseFails
                       PParseMore a' ->
                           next (PParseMore (a' >>= mkB)))
    fail e = PParser 0 (\cont -> cont PParseFails) (\_ cont -> cont PParseFails)

instance MonadFail PParser where
    fail e = PParser 0 (\cont -> cont PParseFails) (\_ cont -> cont PParseFails)

satisfy :: Word32 -> (BL.ByteString -> Bool) -> PParser BL.ByteString
satisfy necy check =
    PParser necy
            (\next -> next PParseFails)
            (\d next ->
                 if check d
                 then next (PParseSuccess d)
                 else next PParseFails)

onFail :: a -> PParser a -> PParser a
onFail ret (PParser minLen _ a) =
    PParser minLen (\next -> next (PParseSuccess ret))
                   (\d next -> a d (\case
                                     PParseFails -> next (PParseSuccess ret)
                                     PParseMore x -> next (PParseMore (onFail ret x))
                                     PParseSuccess x -> next (PParseSuccess x)))

onFailResult :: a -> PParseResult a -> PParseResult a
onFailResult a PParseFails = PParseSuccess a
onFailResult _ (PParseSuccess a) = PParseSuccess a
onFailResult a (PParseMore x) = PParseMore (onFail a x)

simultaneous :: Monoid a => PParser a -> PParser a -> PParser a
simultaneous (PParser aLen aOnDone a) (PParser bLen bOnDone b) =
    PParser (min aLen bLen)
            (\next ->
                 aOnDone $ \ra ->
                 bOnDone $ \rb ->
                 processSimul ra rb next)
            (if | aLen == bLen ->
                    \d next ->
                        a d $ \ra ->
                        b d $ \rb ->
                            processSimul ra rb next
                | aLen < bLen ->
                    \d next ->
                        let b' = PParser (bLen - aLen) (\next' -> next' (PParseSuccess mempty))  (\d' -> b (d <> d'))
                        in a d $ \res ->
                            processSimul res (PParseMore b') next
                | otherwise ->
                    \d next ->
                        let a' = PParser (aLen - bLen)  (\next' -> next' (PParseSuccess mempty)) (\d' -> a (d <> d'))
                        in b d $ \res ->
                            processSimul (PParseMore a') res next)
    where
      processSimul :: forall x r. Monoid x => PParseResult x -> PParseResult x -> (PParseResult x -> r) -> r
      processSimul (PParseSuccess ra') (PParseSuccess rb') next =
          next (PParseSuccess (ra' <> rb'))
      processSimul PParseFails b' next = next (onFailResult mempty b')
      processSimul a' PParseFails next = next (onFailResult mempty a')
      processSimul (PParseMore a') (PParseMore b') next =
          next (PParseMore (simultaneous a' b'))
      processSimul (PParseSuccess ra') (PParseMore b') next =
          next (PParseMore (fmap (mappend ra') (onFail mempty b')))
      processSimul (PParseMore a') (PParseSuccess rb') next =
          next (PParseMore (fmap (`mappend` rb') (onFail mempty a')))

testParser :: PParser a -> BL.ByteString -> Maybe (a, BL.ByteString)
testParser (PParser lenReq onDone process) str
    | fromIntegral (BL.length str) < lenReq =
        let f (PParseSuccess x) = Just (x, str)
            f (PParseMore (PParser 0 onDone' _)) =
                onDone' f
            f _ = Nothing
        in onDone f
    | otherwise =
        let (chunk, str') = BL.splitAt (fromIntegral lenReq) str
        in -- trace ("Split " ++ show (chunk, str')) $
           process chunk $
           \case
             PParseFails -> {- trace "Fails" $ -} Nothing
             PParseSuccess x -> {- trace "Succeeds " $ -} Just (x, str')
             PParseMore cont -> {- trace "More" $ -} testParser cont str'

parseAbAb :: PParser Int
parseAbAb = do
  Any found <- simultaneous (satisfy 2 (== "ab") >> pure (Any True)) (pure (Any False))
  if found then (+1) <$> parseAbAb else return 0

parseAbaBab :: PParser Int
parseAbaBab = do
  Any foundAba <- simultaneous (satisfy 3 (== "aba") >> pure (Any True)) (pure (Any False))
  if foundAba
     then do
       Any foundBab <- simultaneous (satisfy 3 (== "bab") >> pure (Any True)) (pure (Any False))
       if foundBab then (+2) <$> parseAbaBab else return 1
     else
         return 0
