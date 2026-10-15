import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.rocketmq.filter.expression.EvaluationContext;
import org.apache.rocketmq.filter.expression.Expression;
import org.apache.rocketmq.filter.parser.SelectorParser;

/**
 * Reference oracle for the Rust SQL92 engine.
 *
 * Reads one case per line from stdin as "<name>=<value>;<name>=<value>\t<expression>" and writes one
 * verdict per line, mirroring how ConsumerFilterManager builds and ExpressionMessageFilter evaluates
 * a subscription: a compile failure never produces a filter, an evaluation failure matches nothing.
 */
public final class SqlFilterDiff {

    /** Same lookup as broker's MessageEvaluationContext: message properties are always strings. */
    private static final class Properties implements EvaluationContext {
        private final Map<String, String> values;

        private Properties(final Map<String, String> values) {
            this.values = values;
        }

        @Override
        public Object get(final String name) {
            return this.values.get(name);
        }

        @Override
        public Map<String, Object> keyValues() {
            final Map<String, Object> copy = new HashMap<String, Object>(this.values.size(), 1);
            for (final Map.Entry<String, String> entry : this.values.entrySet()) {
                copy.put(entry.getKey(), entry.getValue());
            }
            return copy;
        }
    }

    private static String verdict(final String expression, final Map<String, String> properties) {
        final Expression compiled;
        try {
            compiled = SelectorParser.parse(expression);
        } catch (final Throwable ignored) {
            return "PARSE_ERROR";
        }
        if (compiled == null) {
            return "PARSE_NULL";
        }
        final Object result;
        try {
            result = compiled.evaluate(new Properties(properties));
        } catch (final Throwable ignored) {
            return "EVAL_ERROR";
        }
        if (result == null) {
            return "NULL";
        }
        if (result instanceof Boolean) {
            return ((Boolean) result) ? "TRUE" : "FALSE";
        }
        return "NON_BOOLEAN";
    }

    private static Map<String, String> parseProperties(final String text) {
        final Map<String, String> properties = new HashMap<String, String>();
        if (text.isEmpty()) {
            return properties;
        }
        for (final String pair : text.split(";")) {
            final int separator = pair.indexOf('=');
            properties.put(pair.substring(0, separator), pair.substring(separator + 1));
        }
        return properties;
    }

    public static void main(final String[] args) throws Exception {
        final BufferedReader input =
            new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        final BufferedWriter output =
            new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
        String line;
        while ((line = input.readLine()) != null) {
            final int tab = line.indexOf('\t');
            if (tab < 0) {
                throw new IllegalArgumentException("missing expression separator");
            }
            output.write(verdict(line.substring(tab + 1), parseProperties(line.substring(0, tab))));
            output.write('\n');
        }
        output.flush();
    }
}
