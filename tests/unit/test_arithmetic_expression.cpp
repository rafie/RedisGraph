/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Apache License, Version 2.0,
 * modified with the Commons Clause restriction.
 */

#include "../../deps/googletest/include/gtest/gtest.h"

#ifdef __cplusplus
extern "C" {
#endif

#include "../../src/value.h"
#include "../../src/query_executor.h"
#include "../../src/arithmetic/arithmetic_expression.h"
#include "../../src/graph/entities/node.h"
#include "../../src/arithmetic/agg_funcs.h"
#include "../../src/execution_plan/record.h"
#include "../../src/util/rmalloc.h"

#ifdef __cplusplus
}
#endif

class ArithmeticTest: public ::testing::Test {
  protected:
    static void SetUpTestCase() {
      // Use the malloc family for allocations
      Alloc_Reset();

      AR_RegisterFuncs();
      Agg_RegisterFuncs();
    }

    static void TearDownTestCase() {
    }
};

void _test_string(const AR_ExpNode *exp, const char *expected) {
  char *str;
  AR_EXP_ToString(exp, &str);
  ASSERT_STREQ(str, expected);
  free(str);
}

void _test_ar_func(AR_ExpNode *root, SIValue expected, const Record r) {
  SIValue res = AR_EXP_Evaluate(root, r);
  ASSERT_EQ(res.doubleval, expected.doubleval);
}

AR_ExpNode* _exp_from_query(const char *query) {
  char *errMsg;
  AST *ast = ParseQuery(query, strlen(query), &errMsg);  

  AST_ReturnElementNode *elm = ast->returnNode->returnElements[0];

  AST_ArithmeticExpressionNode *exp = elm->exp;
  AR_ExpNode *arExp = AR_EXP_BuildFromAST(ast, exp);

  AST_Free(ast);
  return arExp;
}

TEST_F(ArithmeticTest, ExpressionTest) {
  SIValue result;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* muchacho */
  query = "RETURN 'muchacho'";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  ASSERT_STREQ(result.stringval, "muchacho");
  AR_EXP_Free(arExp);

  /* 1 */
  query = "RETURN 1";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.doubleval, 1);

  /* 1+2*3 */
  query = "RETURN 1+2*3";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.doubleval, 7);

  /* 1 + 1 + 1 + 1 + 1 + 1 */
  query = "RETURN 1 + 1 + 1 + 1 + 1 + 1";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.doubleval, 6);

  /* ABS(-5 + 2 * 1) */
  query = "RETURN ABS(-5 + 2 * 1)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.doubleval, 3);

  /* 'a' + 'b' */
  query = "RETURN 'a' + 'b'";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_TRUE(strcmp(result.stringval, "ab") == 0);

  /* 1 + 2 + 'a' + 2 + 1 */
  query = "RETURN 1 + 2 + 'a' + 2 + 1";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_TRUE(strcmp(result.stringval, "3.000000a2.0000001.000000") == 0);

  /* 2 * 2 + 'a' + 3 * 3 */
  query = "RETURN 2 * 2 + 'a' + 3 * 3";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_TRUE(strcmp(result.stringval, "4.000000a9.000000") == 0);
}

TEST_F(ArithmeticTest, AggregateTest) {
  SIValue result;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* SUM(1) */
  query = "RETURN SUM(1)";
  arExp = _exp_from_query(query);

  AR_EXP_Aggregate(arExp, r);
  AR_EXP_Aggregate(arExp, r);
  AR_EXP_Aggregate(arExp, r);
  AR_EXP_Reduce(arExp);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.doubleval, 3);

  /* 2+SUM(1) */
  query = "RETURN 2+SUM(1)";
  arExp = _exp_from_query(query);
  AR_EXP_Aggregate(arExp, r);
  AR_EXP_Aggregate(arExp, r);
  AR_EXP_Aggregate(arExp, r);
  AR_EXP_Reduce(arExp);
  /* Just for the kick of it, call reduce more than once.*/
  AR_EXP_Reduce(arExp);
  
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);

  ASSERT_EQ(result.doubleval, 5);
}

TEST_F(ArithmeticTest, AbsTest) {
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* ABS(1) */
  query = "RETURN ABS(1)";
  arExp = _exp_from_query(query);
  SIValue expected = SI_DoubleVal(1);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* ABS(-1) */
  query = "RETURN ABS(-1)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(1);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* ABS(0) */
  query = "RETURN ABS(0)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(0);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);
  
  /* ABS() */
  query = "RETURN ABS(NULL)";
  arExp = _exp_from_query(query);
  expected = SI_NullVal();
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);
}

TEST_F(ArithmeticTest, CeilTest) {
  SIValue expected;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* CEIL(0.5) */
  query = "RETURN CEIL(0.5)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(1);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* CEIL(1) */
  query = "RETURN CEIL(1)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(1);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* CEIL(0.1) */
  query = "RETURN CEIL(0.1)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(1);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* CEIL() */
  query = "RETURN CEIL(NULL)";
  arExp = _exp_from_query(query);
  expected = SI_NullVal();
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);
}

TEST_F(ArithmeticTest, FloorTest) {
  SIValue expected;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* FLOOR(0.5) */
  query = "RETURN FLOOR(0.5)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(0);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* FLOOR(1) */
  query = "RETURN FLOOR(1)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(1);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* FLOOR(0.1) */
  query = "RETURN FLOOR(0.1)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(0);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* FLOOR() */
  query = "RETURN FLOOR(NULL)";
  arExp = _exp_from_query(query);
  expected = SI_NullVal();
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);
}

TEST_F(ArithmeticTest, RoundTest) {
  SIValue expected;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* ROUND(0) */
  query = "RETURN ROUND(0)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(0);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* ROUND(0.49) */
  query = "RETURN ROUND(0.49)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(0);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* ROUND(0.5) */
  query = "RETURN ROUND(0.5)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(1);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* ROUND(1) */
  query = "RETURN ROUND(1)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(1);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* ROUND() */
  query = "RETURN ROUND(NULL)";
  arExp = _exp_from_query(query);
  expected = SI_NullVal();
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);
}

TEST_F(ArithmeticTest, SignTest) {
  SIValue expected;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* SIGN(0) */
  query = "RETURN SIGN(0)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(0);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* SIGN(-1) */
  query = "RETURN SIGN(-1)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(-1);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* SIGN(1) */
  query = "RETURN SIGN(1)";
  arExp = _exp_from_query(query);
  expected = SI_DoubleVal(1);
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);

  /* SIGN() */
  query = "RETURN SIGN(NULL)";
  arExp = _exp_from_query(query);
  expected = SI_NullVal();
  _test_ar_func(arExp, expected, r);
  AR_EXP_Free(arExp);
}

TEST_F(ArithmeticTest, ReverseTest) {
  SIValue result;
  const char *expected;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* REVERSE("muchacho") */
  query = "RETURN REVERSE('muchacho')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "ohcahcum";
  ASSERT_STREQ(result.stringval, expected);

  /* REVERSE("") */
  query = "RETURN REVERSE('')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "";
  ASSERT_STREQ(result.stringval, expected);

  /* REVERSE() */
  query = "RETURN REVERSE(NULL)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, LeftTest) {
  SIValue result;
  const char *expected;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* LEFT("muchacho", 4) */
  query = "RETURN LEFT('muchacho', 4)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "much";
  ASSERT_STREQ(result.stringval, expected);

  /* LEFT("muchacho", 100) */
  query = "RETURN LEFT('muchacho', 100)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "muchacho";
  ASSERT_STREQ(result.stringval, expected);

  /* LEFT(NULL, 100) */
  query = "RETURN LEFT(NULL, 100)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, RightTest) {
  SIValue result;
  const char *expected;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* RIGHT("muchacho", 4) */
  query = "RETURN RIGHT('muchacho', 4)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "acho";
  ASSERT_STREQ(result.stringval, expected);

  /* RIGHT("muchacho", 100) */
  query = "RETURN RIGHT('muchacho', 100)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "muchacho";
  ASSERT_STREQ(result.stringval, expected);

  /* RIGHT(NULL, 100) */
  query = "RETURN RIGHT(NULL, 100)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, LTrimTest) {
  SIValue result;
  const char *expected;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* lTrim("   muchacho") */
  query = "RETURN lTrim('   muchacho')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "muchacho";
  ASSERT_STREQ(result.stringval, expected);

  /* lTrim("muchacho   ") */
  query = "RETURN lTrim('muchacho   ')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "muchacho   ";
  ASSERT_STREQ(result.stringval, expected);

  /* lTrim("   much   acho   ") */
  query = "RETURN lTrim('   much   acho   ')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "much   acho   ";
  ASSERT_STREQ(result.stringval, expected);

  /* lTrim("muchacho") */
  query = "RETURN lTrim('muchacho')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "muchacho";
  ASSERT_STREQ(result.stringval, expected);

  /* lTrim() */
  query = "RETURN lTrim(NULL)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, RTrimTest) {
  SIValue result;
  const char *expected;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* rTrim("   muchacho") */
  query = "RETURN rTrim('   muchacho')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "   muchacho";
  ASSERT_STREQ(result.stringval, expected);

  /* rTrim("muchacho   ") */
  query = "RETURN rTrim('muchacho   ')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "muchacho";
  ASSERT_STREQ(result.stringval, expected);

  /* rTrim("   much   acho   ") */
  query = "RETURN rTrim('   much   acho   ')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "   much   acho";
  ASSERT_STREQ(result.stringval, expected);

  /* rTrim("muchacho") */
  query = "RETURN rTrim('muchacho')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "muchacho";
  ASSERT_STREQ(result.stringval, expected);

  /* rTrim() */
  query = "RETURN rTrim(NULL)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, TrimTest) {
  SIValue result;
  const char *expected;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* trim("   muchacho") */
  query = "RETURN trim('   muchacho')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "muchacho";
  ASSERT_STREQ(result.stringval, expected);

  /* trim("muchacho   ") */
  query = "RETURN trim('muchacho   ')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "muchacho";
  ASSERT_STREQ(result.stringval, expected);

  /* trim("   much   acho   ") */
  query = "RETURN trim('   much   acho   ')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "much   acho";
  ASSERT_STREQ(result.stringval, expected);

  /* trim("muchacho") */
  query = "RETURN trim('muchacho')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "muchacho";
  ASSERT_STREQ(result.stringval, expected);

  /* trim() */
  query = "RETURN trim(NULL)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, SubstringTest) {
  SIValue result;
  const char *expected;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* SUBSTRING("muchacho", 0, 4) */
  query = "RETURN SUBSTRING('muchacho', 0, 4)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "much";
  ASSERT_STREQ(result.stringval, expected);

  /* SUBSTRING("muchacho", 3, 20) */
  query = "RETURN SUBSTRING('muchacho', 3, 20)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "hacho";
  ASSERT_STREQ(result.stringval, expected);

  /* SUBSTRING(NULL, 3, 20) */
  query = "RETURN SUBSTRING(NULL, 3, 20)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, ToLowerTest) {
  SIValue result;
  const char *expected;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* toLower("MuChAcHo") */
  query = "RETURN toLower('MuChAcHo')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "muchacho";
  ASSERT_STREQ(result.stringval, expected);

  /* toLower("mUcHaChO") */
  query = "RETURN toLower('mUcHaChO')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "muchacho";
  ASSERT_STREQ(result.stringval, expected);

  /* toLower("mUcHaChO") */
  query = "RETURN toLower(NULL)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, ToUpperTest) {
  SIValue result;
  const char *expected;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* toUpper("MuChAcHo") */
  query = "RETURN toUpper('MuChAcHo')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "MUCHACHO";
  ASSERT_STREQ(result.stringval, expected);

  /* toUpper("mUcHaChO") */
  query = "RETURN toUpper('mUcHaChO')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "MUCHACHO";
  ASSERT_STREQ(result.stringval, expected);

  /* toUpper("mUcHaChO") */
  query = "RETURN toUpper(NULL)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.type, T_NULL);
}

TEST_F(ArithmeticTest, ToStringTest) {
  SIValue result;
  const char *expected;
  const char *query;
  AR_ExpNode *arExp;
  Record r = Record_New(0);
  
  /* toString("muchacho") */
  query = "RETURN toString('muchacho')";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "muchacho";
  ASSERT_STREQ(result.stringval, expected);

  /* toString("3.14") */
  query = "RETURN toString(3.14)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  expected = "3.140000";
  ASSERT_STREQ(result.stringval, expected);

  /* toString() */
  query = "RETURN toString(NULL)";
  arExp = _exp_from_query(query);
  result = AR_EXP_Evaluate(arExp, r);
  AR_EXP_Free(arExp);
  ASSERT_EQ(result.type, T_NULL);
}
