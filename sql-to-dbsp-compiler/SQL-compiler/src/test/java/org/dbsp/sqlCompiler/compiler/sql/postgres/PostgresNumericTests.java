/*
 * Copyright 2023 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.sql.tools.Change;
import org.dbsp.sqlCompiler.compiler.sql.tools.InputOutputChange;
import org.dbsp.sqlCompiler.ir.expression.DBSPZSetExpression;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTuple;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeDecimal;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;
import org.junit.Test;

/* Tests manually adapted from
 * https://github.com/postgres/postgres/blob/master/src/test/regress/expected/numeric.out
 * Since Calcite rounds differently, these tests give slightly different results from Postgres. */
public class PostgresNumericTests extends SqlIoTest {
    protected static final int WIDTH = 38;

    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        String createTables = "CREATE TABLE num_data (id int4, val numeric(" + WIDTH + ",10));\n" +
                "CREATE TABLE num_exp_add (id1 int4, id2 int4, expected numeric(" + WIDTH + ",10));\n" +
                "CREATE TABLE num_exp_sub (id1 int4, id2 int4, expected numeric(" + WIDTH + ",10));\n" +
                "CREATE TABLE num_exp_div (id1 int4, id2 int4, expected numeric(" + WIDTH + ",10));\n" +
                "CREATE TABLE num_exp_mul (id1 int4, id2 int4, expected numeric(" + WIDTH + ",10));\n" +
                "CREATE TABLE num_exp_sqrt (id int4, expected numeric(" + WIDTH + ",10));\n" +
                "CREATE TABLE num_exp_ln (id int4, expected numeric(" + WIDTH + ",10));\n" +
                "CREATE TABLE num_exp_log10 (id int4, expected numeric(" + WIDTH + ",10));\n" +
                "CREATE TABLE num_exp_power_10_ln (id int4, expected numeric(" + WIDTH + ",10));\n";
        // division results are very different because the result type is different in Calcite
        String insert = """
            INSERT INTO num_exp_add VALUES (0,0,'0');
            INSERT INTO num_exp_sub VALUES (0,0,'0');
            INSERT INTO num_exp_mul VALUES (0,0,'0');
            -- INSERT INTO num_exp_div VALUES (0,0,NaN);
            INSERT INTO num_exp_add VALUES (0,1,'0');
            INSERT INTO num_exp_sub VALUES (0,1,'0');
            INSERT INTO num_exp_mul VALUES (0,1,'0');
            INSERT INTO num_exp_div VALUES (0,1,'0');
            INSERT INTO num_exp_add VALUES (0,2,'-34338492.215397047');
            INSERT INTO num_exp_sub VALUES (0,2,'34338492.215397047');
            INSERT INTO num_exp_mul VALUES (0,2,'0');
            INSERT INTO num_exp_div VALUES (0,2,'0');
            INSERT INTO num_exp_add VALUES (0,3,'4.31');
            INSERT INTO num_exp_sub VALUES (0,3,'-4.31');
            INSERT INTO num_exp_mul VALUES (0,3,'0');
            INSERT INTO num_exp_div VALUES (0,3,'0');
            INSERT INTO num_exp_add VALUES (0,4,'7799461.4119');
            INSERT INTO num_exp_sub VALUES (0,4,'-7799461.4119');
            INSERT INTO num_exp_mul VALUES (0,4,'0');
            INSERT INTO num_exp_div VALUES (0,4,'0');
            INSERT INTO num_exp_add VALUES (0,5,'16397.038491');
            INSERT INTO num_exp_sub VALUES (0,5,'-16397.038491');
            INSERT INTO num_exp_mul VALUES (0,5,'0');
            INSERT INTO num_exp_div VALUES (0,5,'0');
            INSERT INTO num_exp_add VALUES (0,6,'93901.57763026');
            INSERT INTO num_exp_sub VALUES (0,6,'-93901.57763026');
            INSERT INTO num_exp_mul VALUES (0,6,'0');
            INSERT INTO num_exp_div VALUES (0,6,'0');
            INSERT INTO num_exp_add VALUES (0,7,'-83028485');
            INSERT INTO num_exp_sub VALUES (0,7,'83028485');
            INSERT INTO num_exp_mul VALUES (0,7,'0');
            INSERT INTO num_exp_div VALUES (0,7,'0');
            INSERT INTO num_exp_add VALUES (0,8,'74881');
            INSERT INTO num_exp_sub VALUES (0,8,'-74881');
            INSERT INTO num_exp_mul VALUES (0,8,'0');
            INSERT INTO num_exp_div VALUES (0,8,'0');
            INSERT INTO num_exp_add VALUES (0,9,'-24926804.045047420');
            INSERT INTO num_exp_sub VALUES (0,9,'24926804.045047420');
            INSERT INTO num_exp_mul VALUES (0,9,'0');
            INSERT INTO num_exp_div VALUES (0,9,'0');
            INSERT INTO num_exp_add VALUES (1,0,'0');
            INSERT INTO num_exp_sub VALUES (1,0,'0');
            INSERT INTO num_exp_mul VALUES (1,0,'0');
            -- INSERT INTO num_exp_div VALUES (1,0,NaN);
            INSERT INTO num_exp_add VALUES (1,1,'0');
            INSERT INTO num_exp_sub VALUES (1,1,'0');
            INSERT INTO num_exp_mul VALUES (1,1,'0');
            INSERT INTO num_exp_div VALUES (1,1,'0');
            INSERT INTO num_exp_add VALUES (1,2,'-34338492.215397047');
            INSERT INTO num_exp_sub VALUES (1,2,'34338492.215397047');
            INSERT INTO num_exp_mul VALUES (1,2,'0');
            INSERT INTO num_exp_div VALUES (1,2,'0');
            INSERT INTO num_exp_add VALUES (1,3,'4.31');
            INSERT INTO num_exp_sub VALUES (1,3,'-4.31');
            INSERT INTO num_exp_mul VALUES (1,3,'0');
            INSERT INTO num_exp_div VALUES (1,3,'0');
            INSERT INTO num_exp_add VALUES (1,4,'7799461.4119');
            INSERT INTO num_exp_sub VALUES (1,4,'-7799461.4119');
            INSERT INTO num_exp_mul VALUES (1,4,'0');
            INSERT INTO num_exp_div VALUES (1,4,'0');
            INSERT INTO num_exp_add VALUES (1,5,'16397.038491');
            INSERT INTO num_exp_sub VALUES (1,5,'-16397.038491');
            INSERT INTO num_exp_mul VALUES (1,5,'0');
            INSERT INTO num_exp_div VALUES (1,5,'0');
            INSERT INTO num_exp_add VALUES (1,6,'93901.57763026');
            INSERT INTO num_exp_sub VALUES (1,6,'-93901.57763026');
            INSERT INTO num_exp_mul VALUES (1,6,'0');
            INSERT INTO num_exp_div VALUES (1,6,'0');
            INSERT INTO num_exp_add VALUES (1,7,'-83028485');
            INSERT INTO num_exp_sub VALUES (1,7,'83028485');
            INSERT INTO num_exp_mul VALUES (1,7,'0');
            INSERT INTO num_exp_div VALUES (1,7,'0');
            INSERT INTO num_exp_add VALUES (1,8,'74881');
            INSERT INTO num_exp_sub VALUES (1,8,'-74881');
            INSERT INTO num_exp_mul VALUES (1,8,'0');
            INSERT INTO num_exp_div VALUES (1,8,'0');
            INSERT INTO num_exp_add VALUES (1,9,'-24926804.045047420');
            INSERT INTO num_exp_sub VALUES (1,9,'24926804.045047420');
            INSERT INTO num_exp_mul VALUES (1,9,'0');
            INSERT INTO num_exp_div VALUES (1,9,'0');
            INSERT INTO num_exp_add VALUES (2,0,'-34338492.215397047');
            INSERT INTO num_exp_sub VALUES (2,0,'-34338492.215397047');
            INSERT INTO num_exp_mul VALUES (2,0,'0');
            -- INSERT INTO num_exp_div VALUES (2,0,NaN);
            INSERT INTO num_exp_add VALUES (2,1,'-34338492.215397047');
            INSERT INTO num_exp_sub VALUES (2,1,'-34338492.215397047');
            INSERT INTO num_exp_mul VALUES (2,1,'0');
            INSERT INTO num_exp_div VALUES (2,1,2);
            INSERT INTO num_exp_add VALUES (2,2,'-68676984.430794094');
            INSERT INTO num_exp_sub VALUES (2,2,'0');
            INSERT INTO num_exp_mul VALUES (2,2,'1179132047626883.5968621358');
            INSERT INTO num_exp_div VALUES (2,2,'1.00000000000000000000');
            INSERT INTO num_exp_add VALUES (2,3,'-34338487.905397047');
            INSERT INTO num_exp_sub VALUES (2,3,'-34338496.525397047');
            INSERT INTO num_exp_mul VALUES (2,3,'-147998901.4483612725');
            INSERT INTO num_exp_div VALUES (2,3,'-7967167.567377');
            INSERT INTO num_exp_add VALUES (2,4,'-26539030.803497047');
            INSERT INTO num_exp_sub VALUES (2,4,'-42137953.627297047');
            INSERT INTO num_exp_mul VALUES (2,4,'-267821744976817.8111137106');
            INSERT INTO num_exp_div VALUES (2,4,'-4.402674');
            INSERT INTO num_exp_add VALUES (2,5,'-34322095.176906047');
            INSERT INTO num_exp_sub VALUES (2,5,'-34354889.253888047');
            INSERT INTO num_exp_mul VALUES (2,5,'-563049578578.769242506736077');
            INSERT INTO num_exp_div VALUES (2,5,'-2094.188669');
            INSERT INTO num_exp_add VALUES (2,6,'-34244590.637766787');
            INSERT INTO num_exp_sub VALUES (2,6,'-34432393.793027307');
            INSERT INTO num_exp_mul VALUES (2,6,'-3224438592470.1844981192');
            INSERT INTO num_exp_div VALUES (2,6,'-365.685998');
            INSERT INTO num_exp_add VALUES (2,7,'-117366977.215397047');
            INSERT INTO num_exp_sub VALUES (2,7,'48689992.784602953');
            INSERT INTO num_exp_mul VALUES (2,7,'2851072985828710.485883795');
            INSERT INTO num_exp_div VALUES (2,7,'.413574');
            INSERT INTO num_exp_add VALUES (2,8,'-34263611.215397047');
            INSERT INTO num_exp_sub VALUES (2,8,'-34413373.215397047');
            INSERT INTO num_exp_mul VALUES (2,8,'-2571300635581.146276407');
            INSERT INTO num_exp_div VALUES (2,8,'-458.574167');
            INSERT INTO num_exp_add VALUES (2,9,'-59265296.260444467');
            INSERT INTO num_exp_sub VALUES (2,9,'-9411688.170349627');
            INSERT INTO num_exp_mul VALUES (2,9,'855948866655588.453741509242968740');
            INSERT INTO num_exp_div VALUES (2,9,'1.377572');
            INSERT INTO num_exp_add VALUES (3,0,'4.31');
            INSERT INTO num_exp_sub VALUES (3,0,'4.31');
            INSERT INTO num_exp_mul VALUES (3,0,'0');
            -- INSERT INTO num_exp_div VALUES (3,0,NaN);
            INSERT INTO num_exp_add VALUES (3,1,'4.31');
            INSERT INTO num_exp_sub VALUES (3,1,'4.31');
            INSERT INTO num_exp_mul VALUES (3,1,'0');
            INSERT INTO num_exp_div VALUES (3,1,'3');
            INSERT INTO num_exp_add VALUES (3,2,'-34338487.905397047');
            INSERT INTO num_exp_sub VALUES (3,2,'34338496.525397047');
            INSERT INTO num_exp_mul VALUES (3,2,'-147998901.4483612725');
            INSERT INTO num_exp_div VALUES (3,2,'-.000000');
            INSERT INTO num_exp_add VALUES (3,3,'8.62');
            INSERT INTO num_exp_sub VALUES (3,3,'0');
            INSERT INTO num_exp_mul VALUES (3,3,'18.5761');
            INSERT INTO num_exp_div VALUES (3,3,'1.00000000000000000000');
            INSERT INTO num_exp_add VALUES (3,4,'7799465.7219');
            INSERT INTO num_exp_sub VALUES (3,4,'-7799457.1019');
            INSERT INTO num_exp_mul VALUES (3,4,'33615678.685289');
            INSERT INTO num_exp_div VALUES (3,4,'.000000');
            INSERT INTO num_exp_add VALUES (3,5,'16401.348491');
            INSERT INTO num_exp_sub VALUES (3,5,'-16392.728491');
            INSERT INTO num_exp_mul VALUES (3,5,'70671.23589621');
            INSERT INTO num_exp_div VALUES (3,5,'.000262');
            INSERT INTO num_exp_add VALUES (3,6,'93905.88763026');
            INSERT INTO num_exp_sub VALUES (3,6,'-93897.26763026');
            INSERT INTO num_exp_mul VALUES (3,6,'404715.7995864206');
            INSERT INTO num_exp_div VALUES (3,6,'.000045');
            INSERT INTO num_exp_add VALUES (3,7,'-83028480.69');
            INSERT INTO num_exp_sub VALUES (3,7,'83028489.31');
            INSERT INTO num_exp_mul VALUES (3,7,'-357852770.35');
            INSERT INTO num_exp_div VALUES (3,7,'-.000000');
            INSERT INTO num_exp_add VALUES (3,8,'74885.31');
            INSERT INTO num_exp_sub VALUES (3,8,'-74876.69');
            INSERT INTO num_exp_mul VALUES (3,8,'322737.11');
            INSERT INTO num_exp_div VALUES (3,8,'.000057');
            INSERT INTO num_exp_add VALUES (3,9,'-24926799.735047420');
            INSERT INTO num_exp_sub VALUES (3,9,'24926808.355047420');
            INSERT INTO num_exp_mul VALUES (3,9,'-107434525.43415438020');
            INSERT INTO num_exp_div VALUES (3,9,'-.000000');
            INSERT INTO num_exp_add VALUES (4,0,'7799461.4119');
            INSERT INTO num_exp_sub VALUES (4,0,'7799461.4119');
            INSERT INTO num_exp_mul VALUES (4,0,'0');
            -- INSERT INTO num_exp_div VALUES (4,0,NaN);
            INSERT INTO num_exp_add VALUES (4,1,'7799461.4119');
            INSERT INTO num_exp_sub VALUES (4,1,'7799461.4119');
            INSERT INTO num_exp_mul VALUES (4,1,'0');
            INSERT INTO num_exp_div VALUES (4,1,'4');
            INSERT INTO num_exp_add VALUES (4,2,'-26539030.803497047');
            INSERT INTO num_exp_sub VALUES (4,2,'42137953.627297047');
            INSERT INTO num_exp_mul VALUES (4,2,'-267821744976817.8111137106');
            INSERT INTO num_exp_div VALUES (4,2,'-.227134');
            INSERT INTO num_exp_add VALUES (4,3,'7799465.7219');
            INSERT INTO num_exp_sub VALUES (4,3,'7799457.1019');
            INSERT INTO num_exp_mul VALUES (4,3,'33615678.685289');
            INSERT INTO num_exp_div VALUES (4,3,'1809619.817146');
            INSERT INTO num_exp_add VALUES (4,4,'15598922.8238');
            INSERT INTO num_exp_sub VALUES (4,4,'0');
            INSERT INTO num_exp_mul VALUES (4,4,'60831598315717.14146161');
            INSERT INTO num_exp_div VALUES (4,4,'1.00000000000000000000');
            INSERT INTO num_exp_add VALUES (4,5,'7815858.450391');
            INSERT INTO num_exp_sub VALUES (4,5,'7783064.373409');
            INSERT INTO num_exp_mul VALUES (4,5,'127888068979.9935054429');
            INSERT INTO num_exp_div VALUES (4,5,'475.662810');
            INSERT INTO num_exp_add VALUES (4,6,'7893362.98953026');
            INSERT INTO num_exp_sub VALUES (4,6,'7705559.83426974');
            INSERT INTO num_exp_mul VALUES (4,6,'732381731243.7451157640');
            INSERT INTO num_exp_div VALUES (4,6,'83.059961');
            INSERT INTO num_exp_add VALUES (4,7,'-75229023.5881');
            INSERT INTO num_exp_sub VALUES (4,7,'90827946.4119');
            INSERT INTO num_exp_mul VALUES (4,7,'-647577464846017.9715');
            INSERT INTO num_exp_div VALUES (4,7,'-.093937');
            INSERT INTO num_exp_add VALUES (4,8,'7874342.4119');
            INSERT INTO num_exp_sub VALUES (4,8,'7724580.4119');
            INSERT INTO num_exp_mul VALUES (4,8,'584031469984.4839');
            INSERT INTO num_exp_div VALUES (4,8,'104.158082');
            INSERT INTO num_exp_add VALUES (4,9,'-17127342.633147420');
            INSERT INTO num_exp_sub VALUES (4,9,'32726265.456947420');
            INSERT INTO num_exp_mul VALUES (4,9,'-194415646271340.1815956522');
            INSERT INTO num_exp_div VALUES (4,9,'-.312894');
            INSERT INTO num_exp_add VALUES (5,0,'16397.038491');
            INSERT INTO num_exp_sub VALUES (5,0,'16397.038491');
            INSERT INTO num_exp_mul VALUES (5,0,'0');
            -- INSERT INTO num_exp_div VALUES (5,0,NaN);
            INSERT INTO num_exp_add VALUES (5,1,'16397.038491');
            INSERT INTO num_exp_sub VALUES (5,1,'16397.038491');
            INSERT INTO num_exp_mul VALUES (5,1,'0');
            INSERT INTO num_exp_div VALUES (5,1,'5');
            INSERT INTO num_exp_add VALUES (5,2,'-34322095.176906047');
            INSERT INTO num_exp_sub VALUES (5,2,'34354889.253888047');
            INSERT INTO num_exp_mul VALUES (5,2,'-563049578578.769242506736077');
            INSERT INTO num_exp_div VALUES (5,2,'-.000477');
            INSERT INTO num_exp_add VALUES (5,3,'16401.348491');
            INSERT INTO num_exp_sub VALUES (5,3,'16392.728491');
            INSERT INTO num_exp_mul VALUES (5,3,'70671.23589621');
            INSERT INTO num_exp_div VALUES (5,3,'3804.417283');
            INSERT INTO num_exp_add VALUES (5,4,'7815858.450391');
            INSERT INTO num_exp_sub VALUES (5,4,'-7783064.373409');
            INSERT INTO num_exp_mul VALUES (5,4,'127888068979.9935054429');
            INSERT INTO num_exp_div VALUES (5,4,'.002102');
            INSERT INTO num_exp_add VALUES (5,5,'32794.076982');
            INSERT INTO num_exp_sub VALUES (5,5,'0');
            INSERT INTO num_exp_mul VALUES (5,5,'268862871.2753355570');
            INSERT INTO num_exp_div VALUES (5,5,'1.00000000000000000000');
            INSERT INTO num_exp_add VALUES (5,6,'110298.61612126');
            INSERT INTO num_exp_sub VALUES (5,6,'-77504.53913926');
            INSERT INTO num_exp_mul VALUES (5,6,'1539707782.76899778633766');
            INSERT INTO num_exp_div VALUES (5,6,'.174619');
            INSERT INTO num_exp_add VALUES (5,7,'-83012087.961509');
            INSERT INTO num_exp_sub VALUES (5,7,'83044882.038491');
            INSERT INTO num_exp_mul VALUES (5,7,'-1361421264394.416135');
            INSERT INTO num_exp_div VALUES (5,7,'-.000197');
            INSERT INTO num_exp_add VALUES (5,8,'91278.038491');
            INSERT INTO num_exp_sub VALUES (5,8,'-58483.961509');
            INSERT INTO num_exp_mul VALUES (5,8,'1227826639.244571');
            INSERT INTO num_exp_div VALUES (5,8,'.218974');
            INSERT INTO num_exp_add VALUES (5,9,'-24910407.006556420');
            INSERT INTO num_exp_sub VALUES (5,9,'24943201.083538420');
            INSERT INTO num_exp_mul VALUES (5,9,'-408725765384.257043660243220');
            INSERT INTO num_exp_div VALUES (5,9,'-.000657');
            INSERT INTO num_exp_add VALUES (6,0,'93901.57763026');
            INSERT INTO num_exp_sub VALUES (6,0,'93901.57763026');
            INSERT INTO num_exp_mul VALUES (6,0,'0');
            -- INSERT INTO num_exp_div VALUES (6,0,NaN);
            INSERT INTO num_exp_add VALUES (6,1,'93901.57763026');
            INSERT INTO num_exp_sub VALUES (6,1,'93901.57763026');
            INSERT INTO num_exp_mul VALUES (6,1,'0');
            INSERT INTO num_exp_div VALUES (6,1,'6');
            INSERT INTO num_exp_add VALUES (6,2,'-34244590.637766787');
            INSERT INTO num_exp_sub VALUES (6,2,'34432393.793027307');
            INSERT INTO num_exp_mul VALUES (6,2,'-3224438592470.1844981192');
            INSERT INTO num_exp_div VALUES (6,2,'-.002734');
            INSERT INTO num_exp_add VALUES (6,3,'93905.88763026');
            INSERT INTO num_exp_sub VALUES (6,3,'93897.26763026');
            INSERT INTO num_exp_mul VALUES (6,3,'404715.7995864206');
            INSERT INTO num_exp_div VALUES (6,3,'21786.908962');
            INSERT INTO num_exp_add VALUES (6,4,'7893362.98953026');
            INSERT INTO num_exp_sub VALUES (6,4,'-7705559.83426974');
            INSERT INTO num_exp_mul VALUES (6,4,'732381731243.7451157640');
            INSERT INTO num_exp_div VALUES (6,4,'.012039');
            INSERT INTO num_exp_add VALUES (6,5,'110298.61612126');
            INSERT INTO num_exp_sub VALUES (6,5,'77504.53913926');
            INSERT INTO num_exp_mul VALUES (6,5,'1539707782.76899778633766');
            INSERT INTO num_exp_div VALUES (6,5,'5.726740');
            INSERT INTO num_exp_add VALUES (6,6,'187803.15526052');
            INSERT INTO num_exp_sub VALUES (6,6,'0');
            INSERT INTO num_exp_mul VALUES (6,6,'8817506281.4517452372');
            INSERT INTO num_exp_div VALUES (6,6,'1.00000000000000000000');
            INSERT INTO num_exp_add VALUES (6,7,'-82934583.42236974');
            INSERT INTO num_exp_sub VALUES (6,7,'83122386.57763026');
            INSERT INTO num_exp_mul VALUES (6,7,'-7796505729750.37795610');
            INSERT INTO num_exp_div VALUES (6,7,'-.001130');
            INSERT INTO num_exp_add VALUES (6,8,'168782.57763026');
            INSERT INTO num_exp_sub VALUES (6,8,'19020.57763026');
            INSERT INTO num_exp_mul VALUES (6,8,'7031444034.53149906');
            INSERT INTO num_exp_div VALUES (6,8,'1.254010');
            INSERT INTO num_exp_add VALUES (6,9,'-24832902.467417160');
            INSERT INTO num_exp_sub VALUES (6,9,'25020705.622677680');
            INSERT INTO num_exp_mul VALUES (6,9,'-2340666225110.29929521292692920');
            INSERT INTO num_exp_div VALUES (6,9,'-.003767');
            INSERT INTO num_exp_add VALUES (7,0,'-83028485');
            INSERT INTO num_exp_sub VALUES (7,0,'-83028485');
            INSERT INTO num_exp_mul VALUES (7,0,'0');
            -- INSERT INTO num_exp_div VALUES (7,0,NaN);
            INSERT INTO num_exp_add VALUES (7,1,'-83028485');
            INSERT INTO num_exp_sub VALUES (7,1,'-83028485');
            INSERT INTO num_exp_mul VALUES (7,1,'0');
            INSERT INTO num_exp_div VALUES (7,1,'7');
            INSERT INTO num_exp_add VALUES (7,2,'-117366977.215397047');
            INSERT INTO num_exp_sub VALUES (7,2,'-48689992.784602953');
            INSERT INTO num_exp_mul VALUES (7,2,'2851072985828710.485883795');
            INSERT INTO num_exp_div VALUES (7,2,'2.417942');
            INSERT INTO num_exp_add VALUES (7,3,'-83028480.69');
            INSERT INTO num_exp_sub VALUES (7,3,'-83028489.31');
            INSERT INTO num_exp_mul VALUES (7,3,'-357852770.35');
            INSERT INTO num_exp_div VALUES (7,3,'-19264149.651972');
            INSERT INTO num_exp_add VALUES (7,4,'-75229023.5881');
            INSERT INTO num_exp_sub VALUES (7,4,'-90827946.4119');
            INSERT INTO num_exp_mul VALUES (7,4,'-647577464846017.9715');
            INSERT INTO num_exp_div VALUES (7,4,'-10.645412');
            INSERT INTO num_exp_add VALUES (7,5,'-83012087.961509');
            INSERT INTO num_exp_sub VALUES (7,5,'-83044882.038491');
            INSERT INTO num_exp_mul VALUES (7,5,'-1361421264394.416135');
            INSERT INTO num_exp_div VALUES (7,5,'-5063.626888');
            INSERT INTO num_exp_add VALUES (7,6,'-82934583.42236974');
            INSERT INTO num_exp_sub VALUES (7,6,'-83122386.57763026');
            INSERT INTO num_exp_mul VALUES (7,6,'-7796505729750.37795610');
            INSERT INTO num_exp_div VALUES (7,6,'-884.207561');
            INSERT INTO num_exp_add VALUES (7,7,'-166056970');
            INSERT INTO num_exp_sub VALUES (7,7,'0');
            INSERT INTO num_exp_mul VALUES (7,7,'6893729321395225');
            INSERT INTO num_exp_div VALUES (7,7,'1.000000000');
            INSERT INTO num_exp_add VALUES (7,8,'-82953604');
            INSERT INTO num_exp_sub VALUES (7,8,'-83103366');
            INSERT INTO num_exp_mul VALUES (7,8,'-6217255985285');
            INSERT INTO num_exp_div VALUES (7,8,'-1108.805771');
            INSERT INTO num_exp_add VALUES (7,9,'-107955289.045047420');
            INSERT INTO num_exp_sub VALUES (7,9,'-58101680.954952580');
            INSERT INTO num_exp_mul VALUES (7,9,'2069634775752159.035758700');
            INSERT INTO num_exp_div VALUES (7,9,'3.330891');
            INSERT INTO num_exp_add VALUES (8,0,'74881');
            INSERT INTO num_exp_sub VALUES (8,0,'74881');
            INSERT INTO num_exp_mul VALUES (8,0,'0');
            -- INSERT INTO num_exp_div VALUES (8,0,NaN);
            INSERT INTO num_exp_add VALUES (8,1,'74881');
            INSERT INTO num_exp_sub VALUES (8,1,'74881');
            INSERT INTO num_exp_mul VALUES (8,1,'0');
            INSERT INTO num_exp_div VALUES (8,1,'8');
            INSERT INTO num_exp_add VALUES (8,2,'-34263611.215397047');
            INSERT INTO num_exp_sub VALUES (8,2,'34413373.215397047');
            INSERT INTO num_exp_mul VALUES (8,2,'-2571300635581.146276407');
            INSERT INTO num_exp_div VALUES (8,2,'-.002180');
            INSERT INTO num_exp_add VALUES (8,3,'74885.31');
            INSERT INTO num_exp_sub VALUES (8,3,'74876.69');
            INSERT INTO num_exp_mul VALUES (8,3,'322737.11');
            INSERT INTO num_exp_div VALUES (8,3,'17373.781902');
            INSERT INTO num_exp_add VALUES (8,4,'7874342.4119');
            INSERT INTO num_exp_sub VALUES (8,4,'-7724580.4119');
            INSERT INTO num_exp_mul VALUES (8,4,'584031469984.4839');
            INSERT INTO num_exp_div VALUES (8,4,'.009600');
            INSERT INTO num_exp_add VALUES (8,5,'91278.038491');
            INSERT INTO num_exp_sub VALUES (8,5,'58483.961509');
            INSERT INTO num_exp_mul VALUES (8,5,'1227826639.244571');
            INSERT INTO num_exp_div VALUES (8,5,'4.566739');
            INSERT INTO num_exp_add VALUES (8,6,'168782.57763026');
            INSERT INTO num_exp_sub VALUES (8,6,'-19020.57763026');
            INSERT INTO num_exp_mul VALUES (8,6,'7031444034.53149906');
            INSERT INTO num_exp_div VALUES (8,6,'.797441');
            INSERT INTO num_exp_add VALUES (8,7,'-82953604');
            INSERT INTO num_exp_sub VALUES (8,7,'83103366');
            INSERT INTO num_exp_mul VALUES (8,7,'-6217255985285');
            INSERT INTO num_exp_div VALUES (8,7,'-.000901');
            INSERT INTO num_exp_add VALUES (8,8,'149762');
            INSERT INTO num_exp_sub VALUES (8,8,'0');
            INSERT INTO num_exp_mul VALUES (8,8,'5607164161');
            INSERT INTO num_exp_div VALUES (8,8,'1.000000');
            INSERT INTO num_exp_add VALUES (8,9,'-24851923.045047420');
            INSERT INTO num_exp_sub VALUES (8,9,'25001685.045047420');
            INSERT INTO num_exp_mul VALUES (8,9,'-1866544013697.195857020');
            INSERT INTO num_exp_div VALUES (8,9,'-.003004');
            INSERT INTO num_exp_add VALUES (9,0,'-24926804.045047420');
            INSERT INTO num_exp_sub VALUES (9,0,'-24926804.045047420');
            INSERT INTO num_exp_mul VALUES (9,0,'0');
            -- INSERT INTO num_exp_div VALUES (9,0,NaN);
            INSERT INTO num_exp_add VALUES (9,1,'-24926804.045047420');
            INSERT INTO num_exp_sub VALUES (9,1,'-24926804.045047420');
            INSERT INTO num_exp_mul VALUES (9,1,'0');
            INSERT INTO num_exp_div VALUES (9,1,'9');
            INSERT INTO num_exp_add VALUES (9,2,'-59265296.260444467');
            INSERT INTO num_exp_sub VALUES (9,2,'9411688.170349627');
            INSERT INTO num_exp_mul VALUES (9,2,'855948866655588.453741509242968740');
            INSERT INTO num_exp_div VALUES (9,2,'.725914');
            INSERT INTO num_exp_add VALUES (9,3,'-24926799.735047420');
            INSERT INTO num_exp_sub VALUES (9,3,'-24926808.355047420');
            INSERT INTO num_exp_mul VALUES (9,3,'-107434525.43415438020');
            INSERT INTO num_exp_div VALUES (9,3,'-5783481.216948');
            INSERT INTO num_exp_add VALUES (9,4,'-17127342.633147420');
            INSERT INTO num_exp_sub VALUES (9,4,'-32726265.456947420');
            INSERT INTO num_exp_mul VALUES (9,4,'-194415646271340.1815956522');
            INSERT INTO num_exp_div VALUES (9,4,'-3.195964');
            INSERT INTO num_exp_add VALUES (9,5,'-24910407.006556420');
            INSERT INTO num_exp_sub VALUES (9,5,'-24943201.083538420');
            INSERT INTO num_exp_mul VALUES (9,5,'-408725765384.257043660243220');
            INSERT INTO num_exp_div VALUES (9,5,'-1520.201593');
            INSERT INTO num_exp_add VALUES (9,6,'-24832902.467417160');
            INSERT INTO num_exp_sub VALUES (9,6,'-25020705.622677680');
            INSERT INTO num_exp_mul VALUES (9,6,'-2340666225110.29929521292692920');
            INSERT INTO num_exp_div VALUES (9,6,'-265.456711');
            INSERT INTO num_exp_add VALUES (9,7,'-107955289.045047420');
            INSERT INTO num_exp_sub VALUES (9,7,'58101680.954952580');
            INSERT INTO num_exp_mul VALUES (9,7,'2069634775752159.035758700');
            INSERT INTO num_exp_div VALUES (9,7,'.300219');
            INSERT INTO num_exp_add VALUES (9,8,'-24851923.045047420');
            INSERT INTO num_exp_sub VALUES (9,8,'-25001685.045047420');
            INSERT INTO num_exp_mul VALUES (9,8,'-1866544013697.195857020');
            INSERT INTO num_exp_div VALUES (9,8,'-332.885565');
            INSERT INTO num_exp_add VALUES (9,9,'-49853608.090094840');
            INSERT INTO num_exp_sub VALUES (9,9,'0');
            INSERT INTO num_exp_mul VALUES (9,9,'621345559900192.420120630048656400');
            INSERT INTO num_exp_div VALUES (9,9,'1.0000000');
            INSERT INTO num_exp_sqrt VALUES (0,'0');
            INSERT INTO num_exp_sqrt VALUES (1,'0');
            INSERT INTO num_exp_sqrt VALUES (2,'5859.9054783671');
            INSERT INTO num_exp_sqrt VALUES (3,'2.0760539492');
            INSERT INTO num_exp_sqrt VALUES (4,'2792.7515843518');
            INSERT INTO num_exp_sqrt VALUES (5,'128.0509214765');
            INSERT INTO num_exp_sqrt VALUES (6,'306.4336431109');
            INSERT INTO num_exp_sqrt VALUES (7,'9111.9967625104');
            INSERT INTO num_exp_sqrt VALUES (8,'273.6439292218');
            INSERT INTO num_exp_sqrt VALUES (9,'4992.6750389993');
            INSERT INTO num_exp_ln VALUES (0,NULL);
            INSERT INTO num_exp_ln VALUES (1,NULL);
            INSERT INTO num_exp_ln VALUES (2,'17.3517775049');
            INSERT INTO num_exp_ln VALUES (3,'1.4609379041');
            INSERT INTO num_exp_ln VALUES (4,'15.8695652395');
            INSERT INTO num_exp_ln VALUES (5,'9.7048560176');
            INSERT INTO num_exp_ln VALUES (6,'11.4500024662');
            INSERT INTO num_exp_ln VALUES (7,'18.2346942996');
            INSERT INTO num_exp_ln VALUES (8,'11.2236554657');
            INSERT INTO num_exp_ln VALUES (9,'17.0314542501');
            INSERT INTO num_exp_log10 VALUES (0,NULL);
            INSERT INTO num_exp_log10 VALUES (1,NULL);
            INSERT INTO num_exp_log10 VALUES (2,'7.5357812216');
            INSERT INTO num_exp_log10 VALUES (3,'.6344772701');
            INSERT INTO num_exp_log10 VALUES (4,'6.8920646137');
            INSERT INTO num_exp_log10 VALUES (5,'4.2147654161');
            INSERT INTO num_exp_log10 VALUES (6,'4.9726728888');
            INSERT INTO num_exp_log10 VALUES (7,'7.9192271135');
            INSERT INTO num_exp_log10 VALUES (8,'4.8743716355');
            INSERT INTO num_exp_log10 VALUES (9,'7.3966665996');
            INSERT INTO num_exp_power_10_ln VALUES (0,NULL);
            INSERT INTO num_exp_power_10_ln VALUES (1,NULL);
            INSERT INTO num_exp_power_10_ln VALUES (2,'224790267919917955.13261618583642653184');
            INSERT INTO num_exp_power_10_ln VALUES (3,'28.90266599445155957393');
            INSERT INTO num_exp_power_10_ln VALUES (4,'7405685069594999.07733999469386277636');
            INSERT INTO num_exp_power_10_ln VALUES (5,'5068226527.32127265408584640098');
            INSERT INTO num_exp_power_10_ln VALUES (6,'281839893606.99372343357047819067');
            -- INSERT INTO num_exp_power_10_ln VALUES (7,'1716699575118597095.42330819910640247627');
            -- doesn't fit in DECIMAL(28, 10)
            INSERT INTO num_exp_power_10_ln VALUES (8,'167361463828.07491320069016125952');
            INSERT INTO num_exp_power_10_ln VALUES (9,'107511333880052007.04141124673540337457');
            INSERT INTO num_data VALUES (0, '0');
            INSERT INTO num_data VALUES (1, '0');
            INSERT INTO num_data VALUES (2, '-34338492.215397047');
            INSERT INTO num_data VALUES (3, '4.31');
            INSERT INTO num_data VALUES (4, '7799461.4119');
            INSERT INTO num_data VALUES (5, '16397.038491');
            INSERT INTO num_data VALUES (6, '93901.57763026');
            INSERT INTO num_data VALUES (7, '-83028485');
            INSERT INTO num_data VALUES (8, '74881');
            INSERT INTO num_data VALUES (9, '-24926804.045047420');""";
        compiler.submitStatementsForCompilation(createTables);
        compiler.submitStatementsForCompilation(insert);
    }

    /** @param intermediate  A SQL query that defines an intermediate view, which is not output by the circuit.
     * @param last          A SQL query that defines the final view, which is output from the circuit. */
    public void testTwoViews(String intermediate, String last) {
        DBSPCompiler compiler = new DBSPCompiler(this.getOptions(true));
        this.prepareInputs(compiler);
        compiler.submitStatementForCompilation(intermediate);
        compiler.submitStatementForCompilation(last);
        CompilerCircuitStream ccs = this.getCCS(compiler);
        InputOutputChange change = new InputOutputChange(
                this.getPreparedInputs(compiler),
                new Change(
                        DBSPZSetExpression.emptyWithElementType(new DBSPTypeTuple(
                                new DBSPTypeInteger(CalciteObject.EMPTY, 32, true,false),
                                new DBSPTypeInteger(CalciteObject.EMPTY, 64, true,false),
                                new DBSPTypeDecimal(CalciteObject.EMPTY, WIDTH, 10, false),
                                new DBSPTypeDecimal(CalciteObject.EMPTY, WIDTH, 10, false)))));
        ccs.addChange(change);
    }

    @Test
    public void testAdd() {
        String intermediate = "CREATE LOCAL VIEW num_result AS SELECT t1.id AS ID1, t2.id as ID2, "  +
                "CAST(t1.val + t2.val AS NUMERIC(" + WIDTH + ", 10)) AS results\n" +
                "    FROM num_data t1, num_data t2";
        String last = """
                CREATE VIEW E AS SELECT t1.id1, t1.id2, t1.results, t2.expected
                    FROM num_result t1, num_exp_add t2
                    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
                    AND t1.results != t2.expected""";
        this.testTwoViews(intermediate, last);
    }

    @Test
    public void testRoundAdd() {
        String intermediate = "CREATE LOCAL VIEW num_result AS SELECT t1.id AS ID1, t2.id AS ID2, " +
                "CAST(round(t1.val + t2.val, 10) AS NUMERIC(" + WIDTH + ", 10)) AS results\n" +
                "    FROM num_data t1, num_data t2";
        String last = """
                CREATE VIEW E AS SELECT t1.id1, t1.id2, t1.results, round(t2.expected, 10) as expected
                    FROM num_result t1, num_exp_add t2
                    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
                    AND t1.results != round(t2.expected, 10)""";
        this.testTwoViews(intermediate, last);
    }

    @Test
    public void testSubtraction() {
        String intermediate = "CREATE LOCAL VIEW num_result AS SELECT t1.id AS ID1, t2.id AS ID2, " +
                "CAST(t1.val - t2.val AS NUMERIC(" + WIDTH + ", 10)) AS results\n" +
                "    FROM num_data t1, num_data t2";
        String last = """
                CREATE VIEW E AS SELECT t1.id1, t1.id2, t1.results, t2.expected
                    FROM num_result t1, num_exp_sub t2
                    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
                    AND t1.results != t2.expected""";
        this.testTwoViews(intermediate, last);
    }

    @Test
    public void testRoundSubtraction() {
        String intermediate = "CREATE LOCAL VIEW num_result AS SELECT t1.id AS ID1, t2.id AS ID2, " +
                "CAST(ROUND(t1.val - t2.val, 40) AS NUMERIC(" + WIDTH + ",10)) AS results\n" +
                "    FROM num_data t1, num_data t2";
        String last = """
                CREATE VIEW E AS SELECT t1.id1, t1.id2, t1.results, round(t2.expected, 40)
                    FROM num_result t1, num_exp_sub t2
                    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
                    AND t1.results != ROUND(t2.expected, 40)""";
        this.testTwoViews(intermediate, last);
    }

    @Test
    public void testMultiply() {
        String intermediate = "CREATE LOCAL VIEW num_result AS SELECT t1.id AS ID1, t2.id AS ID2, " +
                "CAST(t1.val * t2.val AS NUMERIC(" + WIDTH + ",10)) AS results\n" +
                "    FROM num_data t1, num_data t2";
        String last = """
                CREATE VIEW E AS SELECT t1.id1, t1.id2, t1.results, t2.expected
                    FROM num_result t1, num_exp_mul t2
                    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
                    AND t1.results != t2.expected""";
        this.testTwoViews(intermediate, last);
    }

    @Test
    public void testRoundMultiply() {
        String intermediate = "CREATE LOCAL VIEW num_result AS SELECT t1.id AS ID1, t2.id AS ID2, " +
                "CAST(round(t1.val * t2.val, 30) AS NUMERIC(" + WIDTH + ", 10)) AS results\n" +
                "    FROM num_data t1, num_data t2";
        String last = """
                CREATE VIEW E AS SELECT t1.id1, t1.id2, t1.results, round(t2.expected, 30)
                    FROM num_result t1, num_exp_mul t2
                    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
                    AND t1.results != round(t2.expected, 30)""";
        this.testTwoViews(intermediate, last);
    }

    @Test
    public void testDivision() {
        // Due to calcite rounding rules, the results have effectively scale 6
        String intermediate = "CREATE LOCAL VIEW num_result AS SELECT t1.id AS ID1, t2.id AS ID2, " +
                "CAST(t1.val / t2.val AS NUMERIC(" + WIDTH + ", 10)) AS results\n" +
                "    FROM num_data t1, num_data t2\n" +
                "    WHERE t2.val != 0";
        String last = """
                CREATE VIEW E AS SELECT t1.id1, t1.id2, t1.results, t2.expected
                    FROM num_result t1, num_exp_div t2
                    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
                    AND t1.results != t2.expected""";
        this.testTwoViews(intermediate, last);
    }

    @Test
    public void testDivisionRound() {
        String intermediate = "CREATE LOCAL VIEW num_result AS SELECT t1.id AS ID1, t2.id AS ID2, " +
                "CAST(round(t1.val / t2.val, 10) AS NUMERIC(" + WIDTH + ", 10)) AS results\n" +
                "    FROM num_data t1, num_data t2\n" +
                "    WHERE t2.val != 0";
        String last = """
                CREATE VIEW E AS SELECT t1.id1, t1.id2, t1.results, round(t2.expected, 10)
                    FROM num_result t1, num_exp_div t2
                    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
                    AND t1.results != round(t2.expected, 10)""";
        this.testTwoViews(intermediate, last);
    }

    @Test
    public void squareRootTest() {
        String intermediate = "CREATE LOCAL VIEW num_result AS SELECT id AS ID1, 0 as ID2, CAST(SQRT(ABS(val)) AS NUMERIC(" +
                WIDTH + ", 10)) AS results\n" +
                "    FROM num_data\n";
        String last = """
                CREATE VIEW E AS SELECT t1.id1, t1.results, t2.expected
                    FROM num_result t1, num_exp_sqrt t2
                    WHERE t1.id1 = t2.id
                    AND t1.results != t2.expected""";
        this.testTwoViews(intermediate, last);
    }

    @Test
    public void logarithmTest() {
        String intermediate = "CREATE LOCAL VIEW num_result AS SELECT id AS ID1, 0, CAST(LN(ABS(val)) AS NUMERIC(" +
                WIDTH + ", 10)) AS results\n" +
                "    FROM num_data\n" +
                "    WHERE val != '0.0'";
        String last = """
                CREATE VIEW E AS SELECT t1.id1, t1.results, t2.expected
                    FROM num_result t1, num_exp_ln t2
                    WHERE t1.id1 = t2.id
                    AND t1.results != t2.expected""";
        this.testTwoViews(intermediate, last);
    }

    @Test
    public void logarithm10Test() {
        String intermediate = "CREATE LOCAL VIEW num_result AS SELECT id AS ID1, 0, CAST(LOG10(ABS(val)) AS NUMERIC(" +
                WIDTH + ", 10)) AS results\n" +
                "    FROM num_data\n" +
                "    WHERE val != '0.0'";
        String last = """
                CREATE VIEW E AS SELECT t1.id1, t1.results, t2.expected
                    FROM num_result t1, num_exp_log10 t2
                    WHERE t1.id1 = t2.id
                    AND t1.results != t2.expected""";
        this.testTwoViews(intermediate, last);
    }

    @Test
    public void testCast() {
        this.q("""
               WITH v(x) AS (VALUES(0::numeric),(4.2)) SELECT x FROM v as v1(x);
                x1
               ------
                  0
                4.2""");
    }

    @Test
    public void testNumericArithmetic() {
        // Removed unsupported numeric values inf, nan, etc.
        this.q(
                """
                        WITH v(x) AS (VALUES('0'::numeric(2, 1)),(1),(-1),('4.2'::numeric(4, 2)))
                        SELECT x1, x2,
                          x1 + x2 AS s,
                          x1 - x2 AS diff,
                          x1 * x2 AS prod
                        FROM v AS v1(x1), v AS v2(x2);
                            x1     |    x2     |    sum    |   diff    |   prod
                        -----------+-----------+-----------+-----------+-----------
                                 0 |         0 |         0 |         0 |         0
                                 0 |         1 |         1 |        -1 |         0
                                 0 |        -1 |        -1 |         1 |         0
                                 0 |       4.2 |       4.2 |      -4.2 |       0.0
                                 1 |         0 |         1 |         1 |         0
                                 1 |         1 |         2 |         0 |         1
                                 1 |        -1 |         0 |         2 |        -1
                                 1 |       4.2 |       5.2 |      -3.2 |       4.2
                                -1 |         0 |        -1 |        -1 |         0
                                -1 |         1 |         0 |        -2 |        -1
                                -1 |        -1 |        -2 |         0 |         1
                                -1 |       4.2 |       3.2 |      -5.2 |      -4.2
                               4.2 |         0 |       4.2 |       4.2 |       0.0
                               4.2 |         1 |       5.2 |       3.2 |       4.2
                               4.2 |        -1 |       3.2 |       5.2 |      -4.2
                               4.2 |       4.2 |       8.4 |       0.0 |     17.64""");
    }

    @Test
    public void testCastOutOfRange() {
        this.queryFailingInCompilation("SELECT CAST(1 AS NUMERIC(10, 20)) % 2",
                "Illegal type: DECIMAL type must have scale <= precision");
    }

    @Test
    public void testSpecialValues2Numeric() {
        // Removed unsupported numeric values inf, nan, etc.
        // No div function known, so I removed this one
        this.q("WITH v(x) AS\n" +
                "  (VALUES(CAST(0 AS NUMERIC(" + WIDTH + ", 10))),\n" +
                "         (CAST(1 AS NUMERIC(" + WIDTH + ", 10))),\n" +
                "         (CAST(-1 AS NUMERIC(" + WIDTH + ",10))),\n" +
                "         (CAST(4.2 AS NUMERIC(" + WIDTH + ", 10))))\n" +
                "SELECT x1, x2,\n" +
                "  x1 / x2 AS quot,\n" +
                "  x1 % x2 AS mod\n" +
                // "  div(x1, x2) AS div\n" +
                "FROM v AS v1(x1), v AS v2(x2) WHERE x2 != 0;\n" +
                "    x1     |    x2     |          quot           | mod  \n" +
                "-----------+-----------+-------------------------+------\n" +
                "         0 |         1 |  0.00000000000000000000 |    0\n" +
                "         1 |         1 |  1.00000000000000000000 |    0\n" +
                "        -1 |         1 | -1.00000000000000000000 |    0\n" +
                "       4.2 |         1 |      4.2000000000000000 |  0.2\n" +
                "         0 |        -1 |  0.00000000000000000000 |    0\n" +
                "         1 |        -1 | -1.00000000000000000000 |    0\n" +
                "        -1 |        -1 |  1.00000000000000000000 |    0\n" +
                "       4.2 |        -1 |     -4.2000000000000000 |  0.2\n" +
                "         0 |       4.2 |  0.00000000000000000000 |  0.0\n" +
                "         1 |       4.2 |  0.23809523809523809524 |  1.0\n" +
                "        -1 |       4.2 | -0.23809523809523809524 | -1.0\n" +
                "       4.2 |       4.2 |  1.00000000000000000000 |  0.0");
    }

    @Test
    public void testDivByZero() {
        this.qf("SELECT '0'::numeric / '0'", "Attempt to divide by zero");
        //SELECT 'inf'::numeric % '0';
        this.qf("SELECT '1'::numeric % '0'", "Attempt to modulo by zero");
        //SELECT '-inf'::numeric % '0';
        //SELECT 'nan'::numeric % '0';
        this.qf("SELECT '-1'::numeric % '0'", "Attempt to modulo by zero");
        //SELECT '0'::numeric % '0';
        this.qf("SELECT '0'::numeric % '0'", "Attempt to modulo by zero");
        //SELECT div('inf'::numeric, '0');
        //SELECT div('-inf'::numeric, '0');
        //SELECT div('nan'::numeric, '0');
        //SELECT div('0'::numeric, '0');
    }

    // this is not a postgres test
    @Test
    public void testModuloMinusOne() {
        this.qs("""
                SELECT 2::DECIMAL % -1::DECIMAL;
                 decimal
                ---------
                 0
                (1 row)

                SELECT 2.1::DECIMAL(2, 1) % -1.1::DECIMAL(2, 1);
                 decimal
                ---------
                 1.0
                (1 row)
                """
        );
    }

    @Test
    public void testModulo() {
        this.qs("""
                select 1.12 % 0.3;
                 ?column?
                ----------
                     0.22
                (1 row)

                select 1.12 % -0.3;
                 ?column?
                ----------
                     0.22
                (1 row)

                select -1.12 % 0.3;
                 ?column?
                ----------
                    -0.22
                (1 row)

                select -1.12 % -0.3;
                 ?column?
                ----------
                    -0.22
                (1 row)
                """
        );
    }

    @Test
    public void testFunctionsNumeric0() {
        // dropped unsupported values inf, nan, etc.
        this.q("""
                WITH v(x) AS
                  (VALUES(0),(1),(-1),(4.2),(-7.777))
                SELECT x, -x as minusx, abs(x), floor(x), ceil(x), sign(x)
                FROM v;
                     x     |  minusx   |   abs    |   floor   |   ceil    | sign
                -----------+-----------+----------+-----------+-----------+------
                         0 |         0 |        0 |         0 |         0 |    0
                         1 |        -1 |        1 |         1 |         1 |    1
                        -1 |         1 |        1 |        -1 |        -1 |   -1
                       4.2 |      -4.2 |      4.2 |         4 |         5 |    1
                    -7.777 |     7.777 |    7.777 |        -8 |        -7 |   -1""");
    }

    @Test
    public void testFunctions1() {
        // Removed the unsupported inf, nan, etc. values
        // This test makes no sense for FP
        // 'trunc' has been renamed to 'truncate'
        this.q("""
                WITH v(x) AS
                  (VALUES(0),(1),(-1),(4.2),(-7.777))
                SELECT x, round(x), round(x,1) as round1, truncate(x), truncate(x,1) as trunc1
                FROM v;
                     x     |   round   |  round1   |   trunc   |  trunc1
                -----------+-----------+-----------+-----------+-----------
                         0 |         0 |       0.0 |         0 |       0.0
                         1 |         1 |       1.0 |         1 |       1.0
                        -1 |        -1 |      -1.0 |        -1 |      -1.0
                       4.2 |         4 |       4.2 |         4 |       4.2
                    -7.777 |        -8 |      -7.8 |        -7 |      -7.7""");
    }

    // -- the large values fall into the numeric abbreviation code's maximal classes
    //WITH v(x) AS
    //  (VALUES('0'::numeric),('1'),('-1'),('4.2'),('-7.777'),('1e340'),('-1e340'),
    //         ('inf'),('-inf'),('nan'),
    //         ('inf'),('-inf'),('nan'))
    //SELECT substring(x::text, 1, 32)
    //FROM v ORDER BY x;
    //            substring
    //----------------------------------
    // -Infinity
    // -Infinity
    // -1000000000000000000000000000000
    // -7.777
    // -1
    // 0
    // 1
    // 4.2
    // 10000000000000000000000000000000
    // Infinity
    // Infinity
    // NaN
    // NaN
    //(13 rows)

    @Test
    public void testSqrt() {
        // Removed 'inf' and 'nan'.
        // Interestingly, sqrt in Calcite returns a FP value.
        this.q("""
                WITH v(x) AS
                  (VALUES(0),(1),(4.2))
                SELECT x, sqrt(x)
                FROM v;
                    x     |       sqrt
                ----------+-------------------
                        0 | 0.000000000000000
                        1 | 1.000000000000000
                      4.2 | 2.049390153191920""");
    }

    @Test
    public void testExp() {
        this.qs("""
                --
                -- Tests for EXP()
                --
                -- special cases
                select ROUND(exp(0.0), 12);
                        exp
                --------------------
                 1.0
                (1 row)

                select ROUND(exp(1.0), 12);
                        exp
                --------------------
                 2.718281828459
                (1 row)

                select exp(1.0::numeric(25, 10)); -- changed the precision and scale
                        exp
                --------------------
                 2.7182818284590452
                (1 row)

                select exp(-32.999);
                         exp
                -----------------------
                 4.663547361468238E-15
                (1 row)

                select ROUND(exp(-123.456), 12);
                 exp
                -----
                 0.0
                (1 row)

                -- big test
                select exp(1234.5678);
                 exp
                -----
                 Infinity
                (1 row)
                """
        );
    }

    @Test
    public void testSqrtError() {
        this.qs("""
                SELECT sqrt('-1'::numeric), sqrt(-1e0);
                 d   | d
                -----------
                 NaN | NaN
                (1 row)
                """);
    }

    @Test
    public void testLog() {
        // Removed 'inf' and 'nan'
        // Changed last digit of ln from 6 to 7
        // log in Calcite is different from log in Postgres
        // log(value) is equivalent to ln(value) in Calcite but log10(value) in Postgres
        this.q("""
                WITH v(x) AS
                  (VALUES(1),(CAST(4.2 AS NUMERIC(20, 10))))
                SELECT x,
                  log(x),
                  log10(x),
                  ln(x)
                FROM v;
                    x     |         log        |     log10         |         ln
                ----------+--------------------+---------------+------------------------
                        1 | 0.0000000000000000 | 0.0000000000000000 | 0.0000000000000000
                      4.2 | 1.4350845252893227 | 0.6232492903979005 | 1.4350845252893227""");
    }
}
