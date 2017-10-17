/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.CompilerUtils;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.IntStream;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.CompilerUtils.defineClass;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.nCopies;

public class BucketFunction
        extends SqlScalarFunction
{
    public static final BucketFunction VARCHAR_BUCKET = new BucketFunction(createUnboundedVarcharType().getTypeSignature(), "find bucket given strings");
    public static final BucketFunction DOUBLE_BUCKET = new BucketFunction(new TypeSignature(StandardTypes.DOUBLE), "find bucket  given double");
    // Run assertFunction("BUCKET( 4.1, 5.1, 6.1)", DOUBLE, 0.0);
    private String description;

    private BucketFunction(TypeSignature type, String description)
    {
        super(new Signature(
                "bucket",
                FunctionKind.SCALAR,
                ImmutableList.of(),
                ImmutableList.of(),
                type,
                ImmutableList.of(type),
                true));
        this.description = description;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        if (arity < 2) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "There must be two or more arguments");
        }

        Class<?> clazz = generateConcat(getSignature().getReturnType(), arity);
        MethodHandle methodHandle = methodHandle(clazz, "bucket", nCopies(arity, double.class).toArray(new Class<?>[arity]));

        return new ScalarFunctionImplementation(
                false,
                nCopies(arity, valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle,
                isDeterministic());
    }

    private static Class<?> generateConcat(TypeSignature type, int arity)
    {
        checkCondition(arity <= 254, NOT_SUPPORTED, "Too many arguments for string concatenation");
        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                CompilerUtils.makeClassName(type.getBase() + "_bucket" + arity + "ScalarFunction"),
                type(Object.class));

        // Generate constructor
        definition.declareDefaultConstructor(a(PRIVATE));

        // Generate concat()
        List<Parameter> parameters = IntStream.range(0, arity)
                .mapToObj(i -> arg("arg" + i, double.class))
                .collect(toImmutableList());

        MethodDefinition method = definition.declareMethod(a(PUBLIC, STATIC), "bucket", type(double.class), parameters);
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable length = scope.declareVariable(int.class, "length");
        body.append(length.set(constantInt(0)));


        Variable result = scope.declareVariable(double.class, "result");
        body.append(result.set(generateGetBucket(parameters)));

        body.getVariable(result).retDouble();

        return defineClass(definition, Object.class, ImmutableMap.of(), new DynamicClassLoader(ConcatFunction.class.getClassLoader()));
    }

    private static BytecodeExpression generateGetBucket(List<Parameter> x)
    {
        return invokeStatic(BucketFunction.class, "getBucket", double.class, x);
    }

    @UsedByGeneratedCode
    public static double getBucket(double a, double b, double c)
    {
        return 0.0;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return description;
    }
}
