{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.Beam.MsSQL.Syntax where

import           Database.Beam.Backend.SQL

import           Data.Int
import           Data.Maybe
import           Data.Text (Text)
import qualified Data.Text.Lazy as TL
import           Data.Text.Lazy.Builder
import           Data.Text.Lazy.Builder.Int
import           Data.Word

newtype MsSQLSyntax
    = MsSQLSyntax
    { fromMsSQLSyntax :: Builder
    } deriving (Semigroup, Monoid)

newtype MsSQLCommandSyntax = MsSQLCommandSyntax { fromMsSQLCommand :: MsSQLSyntax }
newtype MsSQLSelectSyntax = MsSQLSelectSyntax { fromMsSQLSelect :: MsSQLSyntax }
newtype MsSQLInsertSyntax = MsSQLInsertSyntax { fromMsSQLInsert :: MsSQLSyntax }
newtype MsSQLDeleteSyntax = MsSQLDeleteSyntax { fromMsSQLDelete :: MsSQLSyntax }
newtype MsSQLUpdateSyntax = MsSQLUpdateSyntax { fromMsSQLUpdate :: MsSQLSyntax }

newtype MsSQLFieldNameSyntax = MsSQLFieldNameSyntax { fromMsSQLFieldName :: MsSQLSyntax }
newtype MsSQLExpressionSyntax = MsSQLExpressionSyntax { fromMsSQLExpression :: MsSQLSyntax }
newtype MsSQLValueSyntax = MsSQLValueSyntax { fromMsSQLValue :: MsSQLSyntax }
newtype MsSQLInsertValuesSyntax = MsSQLInsertValuesSyntax { fromMsSQLInsertValues :: MsSQLSyntax }
newtype MsSQLSelectTableSyntax = MsSQLSelectTableSyntax { fromMsSQLSelectTable :: MsSQLSyntax }
newtype MsSQLSetQuantifierSyntax = MsSQLSetQuantifierSyntax { fromMsSQLSetQuantifier :: MsSQLSyntax }
newtype MsSQLComparisonQuantifierSyntax = MsSQLComparisonQuantifierSyntax { fromMsSQLComparisonQuantifier :: MsSQLSyntax }
newtype MsSQLExtractFieldSyntax = MsSQLExtractFieldSyntax { fromMsSQLExtractField :: MsSQLSyntax }
newtype MsSQLOrderingSyntax = MsSQLOrderingSyntax { fromMsSQLOrdering :: MsSQLSyntax }
newtype MsSQLFromSyntax = MsSQLFromSyntax { fromMsSQLFrom :: MsSQLSyntax }
newtype MsSQLGroupingSyntax = MsSQLGroupingSyntax { fromMsSQLGrouping :: MsSQLSyntax }
newtype MsSQLTableSourceSyntax = MsSQLTableSourceSyntax { fromMsSQLTableSource :: MsSQLSyntax }
newtype MsSQLProjectionSyntax = MsSQLProjectionSyntax { fromMsSQLProjection :: MsSQLSyntax }
newtype MsSQLDataTypeSyntax = MsSQLDataTypeSyntax { fromMsSQLDataType :: MsSQLSyntax }
newtype MsSQLTableNameSyntax = MsSQLTableNameSyntax { fromMsSQLTableName :: MsSQLSyntax }

instance Eq MsSQLExpressionSyntax where
    _ == _ = False

emit :: Builder -> MsSQLSyntax
emit = MsSQLSyntax

msParens :: MsSQLSyntax -> MsSQLSyntax
msParens (MsSQLSyntax a) = MsSQLSyntax ("(" <> a <> ")")

mssqlDomainType, mssqlFieldName, mssqlSchemaName, mssqlTable :: Text -> MsSQLSyntax
mssqlDomainType = mssqlTable
mssqlFieldName = mssqlTable
mssqlSchemaName tblName = emit "[" <> emit (fromText tblName) <> emit "]" -- TODO escape
mssqlTable tblName = emit "[" <> emit (fromText tblName) <> emit "]" -- TODO escape

mssqlSepBy :: MsSQLSyntax -> [MsSQLSyntax] -> MsSQLSyntax
mssqlSepBy _ [] = mempty
mssqlSepBy _ [a] = a
mssqlSepBy sep (a:as) = a <> foldMap (sep <>) as

instance IsSql92Syntax MsSQLCommandSyntax where
    type Sql92SelectSyntax MsSQLCommandSyntax = MsSQLSelectSyntax
    type Sql92InsertSyntax MsSQLCommandSyntax = MsSQLInsertSyntax
    type Sql92UpdateSyntax MsSQLCommandSyntax = MsSQLUpdateSyntax
    type Sql92DeleteSyntax MsSQLCommandSyntax = MsSQLDeleteSyntax

    selectCmd = MsSQLCommandSyntax . fromMsSQLSelect
    insertCmd = MsSQLCommandSyntax . fromMsSQLInsert
    deleteCmd = MsSQLCommandSyntax . fromMsSQLDelete
    updateCmd = MsSQLCommandSyntax . fromMsSQLUpdate

instance IsSql92UpdateSyntax MsSQLUpdateSyntax where
    type Sql92UpdateFieldNameSyntax  MsSQLUpdateSyntax = MsSQLFieldNameSyntax
    type Sql92UpdateExpressionSyntax MsSQLUpdateSyntax = MsSQLExpressionSyntax
    type Sql92UpdateTableNameSyntax  MsSQLUpdateSyntax = MsSQLTableNameSyntax

    updateStmt tbl fields where_ =
      MsSQLUpdateSyntax $
      emit "UPDATE " <> fromMsSQLTableName tbl <>
      (case fields of
         [] -> mempty
         _ -> emit " SET " <>
              mssqlSepBy (emit ", ") (map (\(field, val) -> fromMsSQLFieldName field <> emit "=" <>
                                                            fromMsSQLExpression val) fields)) <>
      maybe mempty (\where' -> emit " WHERE " <> fromMsSQLExpression where') where_

instance IsSql92InsertSyntax MsSQLInsertSyntax where
    type Sql92InsertValuesSyntax MsSQLInsertSyntax = MsSQLInsertValuesSyntax
    type Sql92InsertTableNameSyntax MsSQLInsertSyntax = MsSQLTableNameSyntax

    insertStmt tblName fields values =
      MsSQLInsertSyntax $
      emit "INSERT INTO " <> fromMsSQLTableName tblName <> emit "(" <>
      mssqlSepBy (emit ", ") (map mssqlFieldName fields) <> emit ")" <>
      fromMsSQLInsertValues values

instance IsSql92InsertValuesSyntax MsSQLInsertValuesSyntax where
    type Sql92InsertValuesExpressionSyntax MsSQLInsertValuesSyntax = MsSQLExpressionSyntax
    type Sql92InsertValuesSelectSyntax MsSQLInsertValuesSyntax = MsSQLSelectSyntax

    insertSqlExpressions ess =
        MsSQLInsertValuesSyntax $
        emit "VALUES " <>
        mssqlSepBy (emit ", ")
                   (map (\es -> emit "(" <> mssqlSepBy (emit ", ") (map fromMsSQLExpression es) <> emit ")")
                        ess)

    insertFromSql (MsSQLSelectSyntax a) = MsSQLInsertValuesSyntax a

instance IsSql92DeleteSyntax MsSQLDeleteSyntax where
    type Sql92DeleteExpressionSyntax MsSQLDeleteSyntax = MsSQLExpressionSyntax
    type Sql92DeleteTableNameSyntax MsSQLDeleteSyntax = MsSQLTableNameSyntax

    deleteStmt tbl alias where_  =
      MsSQLDeleteSyntax $
      emit "DELETE FROM " <> fromMsSQLTableName tbl <>
           maybe mempty (\alias' -> emit " AS " <> mssqlTable alias') alias <>
           maybe mempty (\where' -> emit " WHERE " <> fromMsSQLExpression where') where_

instance IsSql92SelectSyntax MsSQLSelectSyntax where
    type Sql92SelectSelectTableSyntax MsSQLSelectSyntax = MsSQLSelectTableSyntax
    type Sql92SelectOrderingSyntax MsSQLSelectSyntax = MsSQLOrderingSyntax

    selectStmt tbl ordering limit offset =
      MsSQLSelectSyntax $
      fromMsSQLSelectTable tbl <>
      (case ordering of
         [] -> mempty
         _ -> emit " ORDER BY " <>
              mssqlSepBy (emit ", ") (map fromMsSQLOrdering ordering)) <>
      case (limit, offset) of
        (Just limit', _) ->
            fakeOrderBy <>
            emit " OFFSET " <> emit (decimal (fromMaybe 0 offset)) <> emit " ROWS" <>
            emit " FETCH NEXT " <> emit (decimal limit') <> emit " ROWS ONLY"
        (Nothing, Just offset') ->
            fakeOrderBy <>
            emit " OFFSET " <> emit (decimal offset') <> emit " ROWS"
        (Nothing, Nothing) -> mempty
      where
        fakeOrderBy = case ordering of
                        [] -> emit " ORDER BY current_timestamp"
                        _ -> mempty

instance IsSql92SelectTableSyntax MsSQLSelectTableSyntax where
    type Sql92SelectTableSelectSyntax MsSQLSelectTableSyntax = MsSQLSelectSyntax
    type Sql92SelectTableExpressionSyntax MsSQLSelectTableSyntax = MsSQLExpressionSyntax
    type Sql92SelectTableProjectionSyntax MsSQLSelectTableSyntax = MsSQLProjectionSyntax
    type Sql92SelectTableFromSyntax MsSQLSelectTableSyntax = MsSQLFromSyntax
    type Sql92SelectTableGroupingSyntax MsSQLSelectTableSyntax = MsSQLGroupingSyntax
    type Sql92SelectTableSetQuantifierSyntax MsSQLSelectTableSyntax = MsSQLSetQuantifierSyntax

    selectTableStmt setQuantifier proj from where_ grouping having =
      MsSQLSelectTableSyntax $
      emit "SELECT " <>
      maybe mempty (\sq' -> fromMsSQLSetQuantifier sq' <> emit " ") setQuantifier <>
      fromMsSQLProjection proj <>
      maybe mempty (emit " FROM " <>) (fmap fromMsSQLFrom from) <>
      maybe mempty (emit " WHERE " <>) (fmap fromMsSQLExpression where_) <>
      maybe mempty (emit " GROUP BY " <>) (fmap fromMsSQLGrouping grouping) <>
      maybe mempty (emit " HAVING " <>) (fmap fromMsSQLExpression having)

    unionTables True = mssqlTblOp "UNION ALL"
    unionTables False = mssqlTblOp "UNION"
    intersectTables _ = mssqlTblOp "INTERSECT"
    exceptTable _ = mssqlTblOp "EXCEPT"

mssqlTblOp :: Builder -> MsSQLSelectTableSyntax -> MsSQLSelectTableSyntax -> MsSQLSelectTableSyntax
mssqlTblOp op a b =
    MsSQLSelectTableSyntax (fromMsSQLSelectTable a <> emit " " <> emit op <> emit " " <>
                            fromMsSQLSelectTable b)

instance IsSql92AggregationSetQuantifierSyntax MsSQLSetQuantifierSyntax where
    setQuantifierDistinct = MsSQLSetQuantifierSyntax (emit "DISTINCT")
    setQuantifierAll = MsSQLSetQuantifierSyntax (emit "ALL")

instance IsSql92FieldNameSyntax MsSQLFieldNameSyntax where
    qualifiedField a b =
        MsSQLFieldNameSyntax (mssqlTable a <> emit "." <> mssqlFieldName b)
    unqualifiedField b = MsSQLFieldNameSyntax (mssqlFieldName b)

instance IsSql92TableNameSyntax MsSQLTableNameSyntax where
    tableName Nothing t = MsSQLTableNameSyntax (mssqlTable t)
    tableName (Just s) t = MsSQLTableNameSyntax (mssqlSchemaName s <> emit "." <> mssqlTable t)

instance IsSql92TableSourceSyntax MsSQLTableSourceSyntax where
    type Sql92TableSourceSelectSyntax MsSQLTableSourceSyntax = MsSQLSelectSyntax
    type Sql92TableSourceTableNameSyntax MsSQLTableSourceSyntax = MsSQLTableNameSyntax
    type Sql92TableSourceExpressionSyntax MsSQLTableSourceSyntax = MsSQLExpressionSyntax

    tableNamed = MsSQLTableSourceSyntax . fromMsSQLTableName
    tableFromSubSelect s = MsSQLTableSourceSyntax (msParens (fromMsSQLSelect s))
    tableFromValues vss = MsSQLTableSourceSyntax $
                          emit "VALUES " <>
                          mssqlSepBy (emit ", ")
                                     (map (msParens . mssqlSepBy (emit ", ") . map fromMsSQLExpression) vss)

instance IsSql92ProjectionSyntax MsSQLProjectionSyntax where
    type Sql92ProjectionExpressionSyntax MsSQLProjectionSyntax = MsSQLExpressionSyntax

    projExprs exprs =
        MsSQLProjectionSyntax $
        mssqlSepBy (emit ", ")
                   (map (\(expr, nm) -> fromMsSQLExpression expr <>
                                        maybe mempty
                                              (\nm' -> emit " AS " <> mssqlFieldName nm')
                                              nm)
                        exprs)

instance IsSql92GroupingSyntax MsSQLGroupingSyntax where
    type Sql92GroupingExpressionSyntax MsSQLGroupingSyntax = MsSQLExpressionSyntax

    groupByExpressions es =
        MsSQLGroupingSyntax $
        mssqlSepBy (emit ", ") (map fromMsSQLExpression es)

instance IsSql92FromSyntax MsSQLFromSyntax where
    type Sql92FromExpressionSyntax MsSQLFromSyntax = MsSQLExpressionSyntax
    type Sql92FromTableSourceSyntax MsSQLFromSyntax = MsSQLTableSourceSyntax

    fromTable tableSource Nothing = MsSQLFromSyntax (fromMsSQLTableSource tableSource)
    fromTable tableSource (Just (nm, cols)) =
        MsSQLFromSyntax $
        fromMsSQLTableSource tableSource <> emit " AS " <> mssqlTable nm <>
        maybe mempty (msParens . mssqlSepBy (emit ", ") . map mssqlFieldName) cols

    innerJoin a b Nothing = MsSQLFromSyntax (fromMsSQLFrom a <> emit " CROSS JOIN " <> fromMsSQLFrom b)
    innerJoin a b (Just e) = mssqlJoin "JOIN" a b (Just e)
    leftJoin = mssqlJoin "LEFT JOIN"
    rightJoin = mssqlJoin "RIGHT JOIN"

instance IsSql92FromOuterJoinSyntax MsSQLFromSyntax where
    outerJoin = mssqlJoin "FULL OUTER JOIN"

mssqlJoin :: Builder -> MsSQLFromSyntax -> MsSQLFromSyntax -> Maybe MsSQLExpressionSyntax -> MsSQLFromSyntax
mssqlJoin joinType a b Nothing =
    MsSQLFromSyntax (fromMsSQLFrom a <> emit " " <> emit joinType <> emit " " <> fromMsSQLFrom b)
mssqlJoin joinType a b (Just on) =
    MsSQLFromSyntax (fromMsSQLFrom a <> emit " " <> emit joinType <> emit " " <> fromMsSQLFrom b <> emit " ON " <> fromMsSQLExpression on)

instance IsSql92OrderingSyntax MsSQLOrderingSyntax where
    type Sql92OrderingExpressionSyntax MsSQLOrderingSyntax = MsSQLExpressionSyntax

    ascOrdering e = MsSQLOrderingSyntax (fromMsSQLExpression e <> emit " ASC")
    descOrdering e = MsSQLOrderingSyntax (fromMsSQLExpression e <> emit " DESC")

instance IsSql92DataTypeSyntax MsSQLDataTypeSyntax where
    domainType nm = MsSQLDataTypeSyntax (mssqlDomainType nm)
    charType prec charSet = MsSQLDataTypeSyntax (emit "CHAR" <> msOptPrec prec <> msOptCharSet charSet)
    varCharType prec charSet = MsSQLDataTypeSyntax (emit "VARCHAR" <> msOptPrec prec <> msOptCharSet charSet)
    nationalCharType prec = MsSQLDataTypeSyntax (emit "NATIONAL CHAR" <> msOptPrec prec)
    nationalVarCharType prec = MsSQLDataTypeSyntax (emit "NATIONAL CHARACTER VARYING" <> msOptPrec prec)

    bitType prec = MsSQLDataTypeSyntax (emit "BIT" <> msOptPrec prec)
    varBitType prec = MsSQLDataTypeSyntax (emit "VARBIT" <> msOptPrec prec)

    numericType prec = MsSQLDataTypeSyntax (emit "NUMERIC" <> msOptNumericPrec prec)
    decimalType prec = MsSQLDataTypeSyntax (emit "DECIMAL" <> msOptNumericPrec prec)

    intType = MsSQLDataTypeSyntax (emit "INT")
    smallIntType = MsSQLDataTypeSyntax (emit "SMALLINT")

    floatType prec = MsSQLDataTypeSyntax (emit "FLOAT" <> msOptPrec prec)
    doubleType = MsSQLDataTypeSyntax (emit "DOUBLE PRECISION")
    realType = MsSQLDataTypeSyntax (emit "REAL")
    dateType = MsSQLDataTypeSyntax (emit "DATE")
    timeType prec withTz = MsSQLDataTypeSyntax (emit "TIME" <> msOptPrec prec <>
                                                if withTz then emit " WITH TIME ZONE" else mempty)
    timestampType prec withTz = MsSQLDataTypeSyntax (emit "TIMESTAMP" <> msOptPrec prec <>
                                                     if withTz then emit " WITH TIME ZONE" else mempty)

msOptPrec :: Maybe Word -> MsSQLSyntax
msOptPrec Nothing = mempty
msOptPrec (Just x) = emit "(" <> emit (decimal x) <> emit ")"

msOptCharSet :: Maybe Text -> MsSQLSyntax
msOptCharSet Nothing = mempty
msOptCharSet (Just cs) = emit " CHARACTER SET " <> emit (fromText cs)

msOptNumericPrec :: Maybe (Word, Maybe Word) -> MsSQLSyntax
msOptNumericPrec Nothing = mempty
msOptNumericPrec (Just (prec, Nothing)) = msOptPrec (Just prec)
msOptNumericPrec (Just (prec, Just dec)) =
    emit "(" <> emit (decimal prec) <> emit ", " <> emit (decimal dec) <> emit ")"

instance IsSql92QuantifierSyntax MsSQLComparisonQuantifierSyntax where
    quantifyOverAll = MsSQLComparisonQuantifierSyntax (emit "ALL")
    quantifyOverAny = MsSQLComparisonQuantifierSyntax (emit "ANY")

instance IsSql92ExtractFieldSyntax MsSQLExtractFieldSyntax where
    secondsField = MsSQLExtractFieldSyntax (emit "SECOND")
    minutesField = MsSQLExtractFieldSyntax (emit "MINUTE")
    hourField    = MsSQLExtractFieldSyntax (emit "HOUR")
    dayField     = MsSQLExtractFieldSyntax (emit "DAY")
    monthField   = MsSQLExtractFieldSyntax (emit "MONTH")
    yearField    = MsSQLExtractFieldSyntax (emit "YEAR")

instance IsSql92ExpressionSyntax MsSQLExpressionSyntax where
    type Sql92ExpressionValueSyntax MsSQLExpressionSyntax = MsSQLValueSyntax
    type Sql92ExpressionSelectSyntax MsSQLExpressionSyntax = MsSQLSelectSyntax
    type Sql92ExpressionFieldNameSyntax MsSQLExpressionSyntax = MsSQLFieldNameSyntax
    type Sql92ExpressionQuantifierSyntax MsSQLExpressionSyntax = MsSQLComparisonQuantifierSyntax
    type Sql92ExpressionCastTargetSyntax MsSQLExpressionSyntax = MsSQLDataTypeSyntax
    type Sql92ExpressionExtractFieldSyntax MsSQLExpressionSyntax = MsSQLExtractFieldSyntax

    addE = msBinOp "+"; subE = msBinOp "-"; mulE = msBinOp "*"; divE = msBinOp "/"

    modE = msBinOp "%"

    orE = msBinOp "OR"; andE = msBinOp "AND"; likeE = msBinOp "LIKE"; overlapsE = msBinOp "OVERLAPS"

    eqE = msCompOp "="; neqE = msCompOp "<>"

    ltE = msCompOp "<"; gtE = msCompOp ">"
    leE = msCompOp "<="; geE = msCompOp ">="
    negateE = msUnOp "-"
    notE = msUnOp "NOT"

    existsE s = MsSQLExpressionSyntax (emit "EXISTS (" <> fromMsSQLSelect s <> emit ")")
    uniqueE s = MsSQLExpressionSyntax (emit "UNIQUE (" <> fromMsSQLSelect s <> emit ")")

    isNotNullE = msPostFix "IS NOT NULL"; isNullE = msPostFix "IS NULL"
    isTrueE = msPostFix "IS TRUE"; isNotTrueE = msPostFix "IS NOT TRUE"
    isFalseE = msPostFix "IS FALSE"; isNotFalseE = msPostFix "IS NOT FALSE"
    isUnknownE = msPostFix "IS UNKNOWN"; isNotUnknownE = msPostFix "IS NOT UNKNOWN"
    betweenE a b c = MsSQLExpressionSyntax (emit "(" <> fromMsSQLExpression a <> emit ") BETWEEN (" <>
                                            fromMsSQLExpression b <> emit ") AND ( " <>
                                            fromMsSQLExpression c <> emit ")")
    valueE = MsSQLExpressionSyntax . fromMsSQLValue
    rowE vs = MsSQLExpressionSyntax (emit "(" <> mssqlSepBy (emit ", ") (map fromMsSQLExpression vs) <> emit ")")

    quantifierListE vs =
      MsSQLExpressionSyntax (emit "(" <> mssqlSepBy (emit ", ") (map (msParens . fromMsSQLExpression) vs) <> emit ")")
    fieldE = MsSQLExpressionSyntax . fromMsSQLFieldName
    subqueryE s = MsSQLExpressionSyntax (msParens (fromMsSQLSelect s))
    positionE needle haystack =
        MsSQLExpressionSyntax (emit "POSITION((" <> fromMsSQLExpression needle <> emit ") IN (" <>
                               fromMsSQLExpression haystack <> emit "))")
    nullIfE a b = MsSQLExpressionSyntax (emit "NULLIF(" <> fromMsSQLExpression a <> emit ", " <>
                                         fromMsSQLExpression b <> emit ")")
    absE x = MsSQLExpressionSyntax (emit "ABS(" <> fromMsSQLExpression x <> emit ")")
    bitLengthE x = MsSQLExpressionSyntax (emit "BIT_LENGTH(" <> fromMsSQLExpression x <> emit ")")
    charLengthE x = MsSQLExpressionSyntax (emit "CHAR_LENGTH(" <> fromMsSQLExpression x <> emit ")")
    octetLengthE x = MsSQLExpressionSyntax (emit "OCTET_LENGTH(" <> fromMsSQLExpression x <> emit ")")
    lowerE x = MsSQLExpressionSyntax (emit "LOWER(" <> fromMsSQLExpression x <> emit ")")
    upperE x = MsSQLExpressionSyntax (emit "UPPER(" <> fromMsSQLExpression x <> emit ")")
    trimE x = MsSQLExpressionSyntax (emit "TRIM(" <> fromMsSQLExpression x <> emit ")")
    coalesceE es = MsSQLExpressionSyntax (emit "COALESCE(" <> mssqlSepBy (emit ", ") (map fromMsSQLExpression es) <> emit ")")
    extractE field from = MsSQLExpressionSyntax (emit "EXTRACT(" <> fromMsSQLExtractField field <> emit " FROM (" <> fromMsSQLExpression from <> emit "))")
    castE e to = MsSQLExpressionSyntax (emit "CAST((" <> fromMsSQLExpression e <> emit ") AS " <> fromMsSQLDataType to <> emit ")")
    caseE cases else' =
        MsSQLExpressionSyntax $
        emit "CASE " <>
        foldMap (\(cond, res) -> emit "WHEN " <> fromMsSQLExpression cond <> emit " THEN " <> fromMsSQLExpression res <> emit " ") cases <>
        emit "ELSE " <> fromMsSQLExpression else' <> emit " END"

    currentTimestampE = MsSQLExpressionSyntax (emit "CURRENT_TIMESTAMP")
    defaultE = MsSQLExpressionSyntax (emit "DEFAULT")

    inE e es = MsSQLExpressionSyntax (msParens (fromMsSQLExpression e) <> emit " IN " <>
                                      msParens (mssqlSepBy (emit ", ") (map fromMsSQLExpression es)))

msBinOp :: Builder -> MsSQLExpressionSyntax -> MsSQLExpressionSyntax -> MsSQLExpressionSyntax
msBinOp op (MsSQLExpressionSyntax a) (MsSQLExpressionSyntax b) =
    MsSQLExpressionSyntax (msParens a <> emit " " <> emit op <> emit " " <> msParens b)

msUnOp :: Builder -> MsSQLExpressionSyntax -> MsSQLExpressionSyntax
msUnOp op (MsSQLExpressionSyntax a) =
    MsSQLExpressionSyntax (emit op <> msParens a)

msPostFix :: Builder -> MsSQLExpressionSyntax -> MsSQLExpressionSyntax
msPostFix op (MsSQLExpressionSyntax a) =
    MsSQLExpressionSyntax (msParens a <> emit " " <> emit op)

msCompOp :: Builder -> Maybe MsSQLComparisonQuantifierSyntax
         -> MsSQLExpressionSyntax -> MsSQLExpressionSyntax
         -> MsSQLExpressionSyntax
msCompOp op q (MsSQLExpressionSyntax a) (MsSQLExpressionSyntax b) =
    MsSQLExpressionSyntax $
    msParens a <> emit op <>
    maybe (msParens b) (\q' -> emit " " <> fromMsSQLComparisonQuantifier q' <> emit " " <> b) q

instance IsSql92AggregationExpressionSyntax MsSQLExpressionSyntax where
    type Sql92AggregationSetQuantifierSyntax MsSQLExpressionSyntax = MsSQLSetQuantifierSyntax

    countAllE = MsSQLExpressionSyntax (emit "COUNT(*)")
    countE = mssqlUnAgg "COUNT"
    avgE = mssqlUnAgg "AVG"
    sumE = mssqlUnAgg "SUM"
    minE = mssqlUnAgg "MIN"
    maxE = mssqlUnAgg "MAX"

mssqlUnAgg :: Builder -> Maybe MsSQLSetQuantifierSyntax
           -> MsSQLExpressionSyntax -> MsSQLExpressionSyntax
mssqlUnAgg fn q e =
    MsSQLExpressionSyntax $
    emit fn <> emit "(" <>
    maybe mempty (\q' -> fromMsSQLSetQuantifier q' <> emit " ") q <>
    fromMsSQLExpression e <> emit ")"

instance HasSqlValueSyntax MsSQLValueSyntax SqlNull where
    sqlValueSyntax _ = MsSQLValueSyntax (emit "NULL")
instance HasSqlValueSyntax MsSQLValueSyntax Bool where
    sqlValueSyntax True  = MsSQLValueSyntax (emit "TRUE")
    sqlValueSyntax False = MsSQLValueSyntax (emit "FALSE")

-- instance HasSqlValueSyntax MsSQLValueSyntax Double where
--     sqlValueSyntax d = MsSQLValueSyntax (emit (doubleDec d))
-- 
-- instance HasSqlValueSyntax MsSQLValueSyntax Float where
--     sqlValueSyntax d = MsSQLValueSyntax (emit (floatDec d))

instance HasSqlValueSyntax MsSQLValueSyntax Int where
    sqlValueSyntax d = MsSQLValueSyntax (emit (decimal d))
instance HasSqlValueSyntax MsSQLValueSyntax Int8 where
    sqlValueSyntax d = MsSQLValueSyntax $ emit (decimal d)
instance HasSqlValueSyntax MsSQLValueSyntax Int16 where
    sqlValueSyntax d = MsSQLValueSyntax $ emit (decimal d)
instance HasSqlValueSyntax MsSQLValueSyntax Int32 where
    sqlValueSyntax d = MsSQLValueSyntax $ emit (decimal d)
instance HasSqlValueSyntax MsSQLValueSyntax Int64 where
    sqlValueSyntax d = MsSQLValueSyntax $ emit (decimal d)
instance HasSqlValueSyntax MsSQLValueSyntax Integer where
    sqlValueSyntax d = MsSQLValueSyntax $ emit (decimal d)

instance HasSqlValueSyntax MsSQLValueSyntax Word where
    sqlValueSyntax d = MsSQLValueSyntax $ emit (decimal d)
instance HasSqlValueSyntax MsSQLValueSyntax Word8 where
    sqlValueSyntax d = MsSQLValueSyntax $ emit (decimal d)
instance HasSqlValueSyntax MsSQLValueSyntax Word16 where
    sqlValueSyntax d = MsSQLValueSyntax $ emit (decimal d)
instance HasSqlValueSyntax MsSQLValueSyntax Word32 where
    sqlValueSyntax d = MsSQLValueSyntax $ emit (decimal d)
instance HasSqlValueSyntax MsSQLValueSyntax Word64 where
    sqlValueSyntax d = MsSQLValueSyntax $ emit (decimal d)

instance HasSqlValueSyntax MsSQLValueSyntax [Char] where
    sqlValueSyntax = sqlValueSyntax . TL.pack

instance HasSqlValueSyntax MsSQLValueSyntax Text where
    sqlValueSyntax = sqlValueSyntax . TL.fromStrict

instance HasSqlValueSyntax MsSQLValueSyntax TL.Text where
    sqlValueSyntax t = MsSQLValueSyntax $ MsSQLSyntax $ fromLazyText ("'" <> TL.concatMap (\c -> if c == '\'' then "''" else TL.pack [c]) t <> "'")

instance HasSqlValueSyntax MsSQLValueSyntax x => HasSqlValueSyntax MsSQLValueSyntax (Maybe x) where
    sqlValueSyntax Nothing = sqlValueSyntax SqlNull
    sqlValueSyntax (Just x) = sqlValueSyntax x
