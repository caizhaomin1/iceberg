/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.dme.spark;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.dme.rest.DmeVectorJsonCodec;
import org.apache.iceberg.dme.vector.DmeVectorDeclarationContext;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;

/** A narrow parser decorator; all SQL other than FLOATVECTOR DDL is delegated unchanged. */
final class DmeSparkSqlParser implements ParserInterface {
  private static final Pattern VECTOR =
      Pattern.compile(
          "(?i)(`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)\\s+FLOATVECTOR\\s*\\(\\s*(\\d+)\\s*\\)");
  private static final Pattern CREATE =
      Pattern.compile("(?is)^\\s*CREATE(?:\\s+OR\\s+REPLACE)?\\s+TABLE\\b");
  private static final Pattern ALTER_ADD =
      Pattern.compile("(?is)^\\s*ALTER\\s+TABLE\\b.*?\\bADD(?:\\s+COLUMN(?:S)?)?\\b");
  private static final Pattern TBLPROPERTIES =
      Pattern.compile("(?is)\\bTBLPROPERTIES\\s*\\((.*)\\)\\s*;?\\s*$");

  private final ParserInterface delegate;

  DmeSparkSqlParser(ParserInterface delegate) {
    this.delegate = delegate;
  }

  @Override
  public LogicalPlan parsePlan(String sqlText) throws ParseException {
    Rewrite rewrite = rewrite(sqlText);
    if (rewrite.alterAdd) {
      DmeVectorDeclarationContext.clear();
      rewrite.declarations.forEach(DmeVectorDeclarationContext::put);
    }
    return delegate.parsePlan(rewrite.sql);
  }

  private static Rewrite rewrite(String sql) {
    Matcher matcher = VECTOR.matcher(sql);
    Map<String, Integer> declarations = new LinkedHashMap<>();
    StringBuffer rewritten = new StringBuffer();
    while (matcher.find()) {
      String name = unquote(matcher.group(1));
      int dimension = Integer.parseInt(matcher.group(2));
      if (dimension <= 0) {
        throw new IllegalArgumentException("FLOATVECTOR dimension must be positive: " + dimension);
      }
      declarations.put(name, dimension);
      matcher.appendReplacement(
          rewritten, Matcher.quoteReplacement(matcher.group(1) + " ARRAY<FLOAT>"));
    }
    matcher.appendTail(rewritten);

    String transformed = rewritten.toString();
    boolean create = CREATE.matcher(sql).find();
    boolean alterAdd = ALTER_ADD.matcher(sql).find();
    if (create && !declarations.isEmpty()) {
      transformed = appendCreateDeclarations(transformed, declarations);
    }
    return new Rewrite(transformed, declarations, alterAdd && !declarations.isEmpty());
  }

  private static String appendCreateDeclarations(String sql, Map<String, Integer> declarations) {
    String encoded =
        declarations.entrySet().stream()
            .map(entry -> entry.getKey() + ":" + entry.getValue())
            .reduce((left, right) -> left + "," + right)
            .orElseThrow();
    Matcher properties = TBLPROPERTIES.matcher(sql);
    String property = "'" + DmeVectorJsonCodec.DECLARATIONS_PROPERTY + "'='" + encoded + "'";
    if (properties.find()) {
      String existing = properties.group(1).trim();
      String replacement =
          "TBLPROPERTIES (" + (existing.isEmpty() ? property : existing + ", " + property) + ")";
      return sql.substring(0, properties.start()) + replacement;
    }
    return sql.replaceFirst(";?\\s*$", " TBLPROPERTIES (" + property + ")");
  }

  private static String unquote(String name) {
    return name.startsWith("`") ? name.substring(1, name.length() - 1) : name;
  }

  @Override
  public Expression parseExpression(String sqlText) throws ParseException {
    return delegate.parseExpression(sqlText);
  }

  @Override
  public TableIdentifier parseTableIdentifier(String sqlText) throws ParseException {
    return delegate.parseTableIdentifier(sqlText);
  }

  @Override
  public FunctionIdentifier parseFunctionIdentifier(String sqlText) throws ParseException {
    return delegate.parseFunctionIdentifier(sqlText);
  }

  @Override
  public Seq<String> parseMultipartIdentifier(String sqlText) throws ParseException {
    return delegate.parseMultipartIdentifier(sqlText);
  }

  @Override
  public LogicalPlan parseQuery(String sqlText) throws ParseException {
    return delegate.parseQuery(sqlText);
  }

  @Override
  public StructType parseTableSchema(String sqlText) throws ParseException {
    return delegate.parseTableSchema(sqlText);
  }

  @Override
  public DataType parseDataType(String sqlText) throws ParseException {
    return delegate.parseDataType(sqlText);
  }

  private static final class Rewrite {
    private final String sql;
    private final Map<String, Integer> declarations;
    private final boolean alterAdd;

    private Rewrite(String sql, Map<String, Integer> declarations, boolean alterAdd) {
      this.sql = sql;
      this.declarations = declarations;
      this.alterAdd = alterAdd;
    }
  }
}
