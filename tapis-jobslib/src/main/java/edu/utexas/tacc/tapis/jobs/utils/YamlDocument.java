package edu.utexas.tacc.tapis.jobs.utils;

import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;


/**
 * A utility for parsing, writing, and modifying YAML documents. The class implements a very simple
 * JSONPath-like (<a href="https://datatracker.ietf.org/doc/html/rfc9535">RFC 9535</a>) query language
 * that treats a YAML document as a tree of nodes, and a query as a path that identifies nodes within
 * that tree. A query consists of zero or more segments separated by dots, and the path defined by
 * these segments is assumed to be relative to the root node. Thus, a query with no segments will match
 * the root of the tree. A named segment selects a node with a matching name. If a selected node is an
 * array, an index enclosed by brackets can be used to specify a particular element of the array. If
 * no element is specified, all elements of the array are selected. A backslash can be used to escape
 * characters. So, given the document
 *
 * one: 1
 * two:
 *   - 2
 *   - three:
 *       - 3
 *       - 4
 *   - five:
 *       six: 6
 *       seven.eight: 7.8
 *
 * a query of 'one' will select a value of '1', a query of 'two[0]' will select a value of '2', a
 * query of 'two.three' will select a value of '[3,4]', a query of 'two.five.six' will select a
 * value of '6', and a query of 'two.five.seven\.eight' will select a value of '7.8'
 *
 * @author phoover
 */
public class YamlDocument
{
    // nested classes


    /**
     * Represents a segment of a query. Extensions of this class each define a
     * selector that, given a set of nodes as input, will produce a set of nodes
     * that are children of the input set
     */
    private static abstract class QuerySegment
    {
        /**
         * Finds child nodes that match the selector for this segment
         *
         * @param nodes a list of document nodes
         * @return a list of document nodes selected by this segment, or null
         * if the selector didn't produce a match
         */
        public abstract List<Object> find(List<Object> nodes);

        /**
         * Sets a value for the child nodes matched by the selector for this
         * segment. If no child matches the selector, then a new node will be
         * created
         *
         * @param nodes a list of document nodes
         * @param value the value to set
         * @return the previous values of the child nodes
         */
        public abstract List<Object> set(List<Object> nodes, Object value);

        /**
         * Appends a value to the child nodes matched by the selector for this
         * segment. If the selector matches a value that is not a list, then a
         * new list node will be created that contains both the existing and
         * appended values. If no child matches the selector, then a new node
         * will be created
         *
         * @param nodes a list of document nodes
         * @param value the value to append
         * @return a list of boolean values that indicate whether or not the
         * child nodes changed as a result of the operation
         */
        public abstract List<Object> append(List<Object> nodes, Object value);

        /**
         * Creates a copy of an object tree. This is necessary because if
         * SnakeYAML detects that the same instance of a collection already
         * exists in the document, it will create an anchor pointing to that
         * instance, rather than a new collection. This can lead to unexpected
         * behavior on subsequent edits.
         *
         * @param node a document node
         * @return a copy of the node and its children
         */
        @SuppressWarnings("unchecked")
        protected Object copy(Object node)
        {
            // traverse the tree in level order
            List<Object> root = new ArrayList<Object>(1);
            Deque<Object> nodes = new ArrayDeque<Object>();

            root.add(node);
            nodes.push(root);

            do {
                Object current = nodes.pop();

                if (current instanceof Map) {
                    for (Map.Entry<String, Object> entry : ((Map<String, Object>) current).entrySet()) {
                        Object value = entry.getValue();
                        Object newValue;

                        // new instances are created for collections, not scalar values
                        if (value instanceof Map)
                            newValue = new LinkedHashMap<String, Object>((Map<String, Object>) value);
                        else if (value instanceof List)
                            newValue = new ArrayList<Object>((List<Object>) value);
                        else
                            continue;

                        ((Map<String, Object>) current).put(entry.getKey(), newValue);
                        nodes.push(newValue);
                    }
                }
                else if (current instanceof List) {
                    for (int index = 0; index < ((List<?>) current).size(); index += 1) {
                        Object element = ((List<?>) current).get(index);
                        Object newElement;

                        // new instances are created for collections, not scalar values
                        if (element instanceof Map)
                            newElement = new LinkedHashMap<String, Object>((Map<String, Object>) element);
                        else if (element instanceof List)
                            newElement = new ArrayList<Object>((List<Object>) element);
                        else
                            continue;

                        ((List<Object>) current).set(index, newElement);
                        nodes.push(newElement);
                    }
                }
            } while (!nodes.isEmpty());

            return root.get(0);
        }
    }

    /**
     * A query segment that uses a name as the selector
     */
    private static class NodeName
      extends QuerySegment
    {
        private final String _name;


        /**
         * Constructor
         *
         * @param name the name to use as the selector
         */
        public NodeName(String name)
        {
            _name = name;
        }

        @Override
        public List<Object> find(List<Object> nodes)
        {
            List<Object> results = new ArrayList<Object>();

            for (Object node : nodes) {
                // if the node is a list, examine its elements to see if any of
                // them are maps that contain the name as a key
                if (node instanceof List) {
                    for (Object element : (List<?>) node) {
                        if (element instanceof Map && ((Map<?, ?>) element).containsKey(_name))
                            results.add(((Map<?, ?>) element).get(_name));
                    }
                }
                else if (node instanceof Map && ((Map<?, ?>) node).containsKey(_name))
                    results.add(((Map<?, ?>) node).get(_name));
            }

            return results;
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<Object> set(List<Object> nodes, Object value)
        {
            List<Object> results = new ArrayList<Object>();

            for (Object node : nodes) {
                // if the node is a list, examine its elements to see if any of
                // them are maps that contain the name as a key
                if (node instanceof List) {
                    for (Object element : (List<?>) node) {
                        if (element instanceof Map) {
                            Object copy = copy(value);

                            results.add(((Map<String, Object>) element).put(_name, copy));
                        }
                    }
                }
                else if (node instanceof Map) {
                    Object copy = copy(value);

                    results.add(((Map<String, Object>) node).put(_name, copy));
                }
            }

            return results;
        }

        @Override
        public List<Object> append(List<Object> nodes, Object value)
        {
            List<Object> results = new ArrayList<Object>();

            for (Object node : nodes) {
                // if the node is a list, examine its elements to see if any of
                // them are maps that contain the name as a key
                if (node instanceof List) {
                    for (Object element : (List<?>) node) {
                        if (element instanceof Map)
                            results.add(append(element, value));
                    }
                }
                else if (node instanceof Map)
                    results.add(append(node, value));
            }

            return results;
        }

        @SuppressWarnings("unchecked")
        private Object append(Object node, Object value)
        {
            Object copy = copy(value);
            Object result;

            // the selector only matches map keys, so node must be some
            // implementation of the Map interface
            if (((Map<?, ?>) node).containsKey(_name)) {
                Object target = ((Map<?, ?>) node).get(_name);

                // if the selected node is a list, the value is appended to
                // it. Otherwise, a new list is created that contains both
                // the selected node and the value
                if (target instanceof List)
                    ((List<Object>) target).add(copy);
                else {
                    List<Object> newTarget = new ArrayList<Object>(2);

                    newTarget.add(target);
                    newTarget.add(copy);
                    ((Map<String, Object>) node).put(_name, newTarget);

                    target = newTarget;
                }

                result = target;
            }
            else {
                ((Map<String, Object>) node).put(_name, copy);

                result = copy;
            }

            return result;
        }
    }

    /**
     * A query segment that uses an array index as a selector
     */
    private static class ArrayIndex
      extends QuerySegment
    {
        private final int _index;


        /**
         * Constructor
         *
         * @param index the array index to use as the selector
         * @throws JobException
         */
        public ArrayIndex(String index) throws JobException
        {
            try {
                _index = Integer.parseInt(index);
            }
            catch (NumberFormatException err) {
                throw new JobException("index is not a number");
            }
        }

        @Override
        public List<Object> find(List<Object> nodes)
        {
            List<Object> results = new ArrayList<Object>();

            for (Object node : nodes) {
                if (node instanceof List && ((List<?>) node).size() > _index)
                    results.add(((List<?>) node).get(_index));
            }

            return results;
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<Object> set(List<Object> nodes, Object value)
        {
            List<Object> results = new ArrayList<Object>();

            for (Object node : nodes) {
                if (node instanceof List && ((List<?>) node).size() > _index) {
                    Object copy = copy(value);

                    results.add(((List<Object>) node).set(_index, copy));
                }
            }

            return results;
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<Object> append(List<Object> nodes, Object value)
        {
            List<Object> results = new ArrayList<Object>();

            for (Object node : nodes) {
                if (node instanceof List && ((List<?>) node).size() > _index) {
                    Object target = ((List<?>) node).get(_index);
                    Object copy = copy(value);

                    // if the selected node is a list, the value is appended to
                    // it. Otherwise, a new list is created that contains both
                    // the selected node and the value
                    if (target instanceof List)
                        ((List<Object>) target).add(copy);
                    else {
                        List<Object> newTarget = new ArrayList<Object>(2);

                        newTarget.add(target);
                        newTarget.add(copy);
                        ((List<Object>) node).set(_index, newTarget);

                        target = newTarget;
                    }

                    results.add(target);
                }
            }

            return results;
        }
    }

    /**
     * A functional interface whose method modifies a set of nodes
     */
    @FunctionalInterface
    private interface NodeModifier
    {
        /**
         * Uses a query segment to select children from a set of input nodes,
         * then modifies them using the given value
         *
         * @param segment a query segment
         * @param nodes a set of nodes to select from
         * @param value a value used to modify the selected nodes
         * @return the results of the modification
         */
        List<Object> modify(QuerySegment segment, List<Object> nodes, Object value);
    }


    // data fields


    private final Yaml _parser;
    private Object _document;


    // constructors


    /**
     * Constructs an empty document
     */
    public YamlDocument()
    {
        DumperOptions options = new DumperOptions();

        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

        _parser = new Yaml(options);
        _document = null;
    }

    /**
     * Constructs a document using the data from an {@link java.io.InputStream}
     *
     * @param input data for the document
     */
    public YamlDocument(InputStream input)
    {
        this();

        load(input);
    }

    /**
     * Constructs a document using data from a {@link java.io.Reader}
     *
     * @param input data for the document
     */
    public YamlDocument(Reader input)
    {
        this();

        load(input);
    }

    /**
     * Constructs a document using data from a {@link java.lang.String}
     *
     * @param input data for the document
     */
    public YamlDocument(String input)
    {
        this();

        load(input);
    }


    // public methods


    /**
     * Creates a document using data from an {@link java.io.InputStream}. The
     * new document replaces any existing document
     *
     * @param input data for the document
     */
    public void load(InputStream input)
    {
        _document = _parser.load(input);
    }

    /**
     * Creates a document using data from a {@link java.io.Reader}. The
     * new document replaces any existing document
     *
     * @param input data for the document
     */
    public void load(Reader input)
    {
        _document = _parser.load(input);
    }

    /**
     * Creates a document using data from a {@link java.lang.String}. The
     * new document replaces any existing document
     *
     * @param input data for the document
     */
    public void load(String input)
    {
        _document = _parser.load(input);
    }

    /**
     * Produces the text of a YAML document using the current object
     * representation
     *
     * @return the text of the document
     */
    public String dump()
    {
        if (_document == null)
            return null;

        return _parser.dump(_document);
    }

    /**
     * Selects nodes from the document using the given query
     *
     * @param path a query
     * @return a node or list of nodes selected by the query, or null if the
     * query didn't produce a match
     * @throws JobException
     */
    public Object getNode(String path) throws JobException
    {
        // an empty query matches the root of the document
        if (path == null || path.isEmpty())
            return _document;

        List<QuerySegment> segments = parsePath(path);
        List<Object> nodes = new ArrayList<Object>();

        nodes.add(_document);

        for (QuerySegment segment : segments) {
            nodes = segment.find(nodes);

            if (nodes.isEmpty())
                return null;
        }

        // if the query matches only one node, just return that node
        if (nodes.size() == 1)
            return nodes.get(0);

        return nodes;
    }

    /**
     * Sets the value of a node or set of nodes indicated by the query
     *
     * @param path a query
     * @param node the value to set
     * @throws JobException
     */
    public void setNode(String path, Object node) throws JobException
    {
        replaceNode(path, node);
    }

    /**
     * Replaces the value of a node or set of nodes indicated by the query
     *
     * @param path a query
     * @param node the value to set
     * @return the previous value of the selected node, or null if the query
     * didn't produce a match
     * @throws JobException
     */
    public Object replaceNode(String path, Object node) throws JobException
    {
        // an empty query matches the root of the document
        if (path == null || path.isEmpty()) {
            Object previous = _document;

            _document = node;

            return previous;
        }

        // uses a lambda expression to define a function that sets the value of the selected nodes
        return modifyNode(path, node, (QuerySegment segment, List<Object> nodes, Object value) -> segment.set(nodes, value));
    }

    /**
     * Appends a value to a node or set of nodes indicated by the query
     *
     * @param path a query
     * @param node the value to append
     * @return the new value of the selected node, or null if the query didn't
     * produce a match
     * @throws JobException
     */
    @SuppressWarnings("unchecked")
    public Object appendNode(String path, Object node) throws JobException
    {
        // an empty query matches the root of the document
        if (path == null || path.isEmpty()) {
            // if the root node is a list, append the new node. Otherwise,
            // create a list using the root node and the new node
            if (_document instanceof List)
                ((List<Object>) _document).add(node);
            else {
                List<Object> newRoot = new ArrayList<Object>(2);

                newRoot.add(_document);
                newRoot.add(node);

                _document = newRoot;
            }

            return _document;
        }

        // uses a lambda expression to define a function that appends the value to the selected nodes
        return modifyNode(path, node, (QuerySegment segment, List<Object> nodes, Object value) -> segment.append(nodes, value));
    }

    /**
     * Selects nodes from the document using the given query
     *
     * @param path a query
     * @return the text of a node or list of nodes selected by the query, or
     * null if the query didn't produce a match
     * @throws JobException
     */
    public String getValue(String path) throws JobException
    {
        Object node = getNode(path);

        if (node == null)
            return null;

        return _parser.dump(node);
    }

    /**
     * Sets the value of a node or set of nodes indicated by the query
     *
     * @param path a query
     * @param value the value to set
     * @throws JobException
     */
    public void setValue(String path, String value) throws JobException
    {
        replaceValue(path, value);
    }

    /**
     * Replaces the value of a node or set of nodes indicated by the query
     *
     * @param path a query
     * @param value the value to set
     * @return the text of the previous value of the selected node, or null if
     * the query didn't produce a match
     * @throws JobException
     */
    public String replaceValue(String path, String value) throws JobException
    {
        Object doc = _parser.load(value);
        Object previous = replaceNode(path, doc);

        if (previous == null)
            return null;

        return _parser.dump(previous);
    }

    /**
     * Appends a value to a node or set of nodes indicated by the query
     *
     * @param path a query
     * @param value the value to append
     * @return the text of the new value of the selected node, or null if the
     * query didn't produce a match
     * @throws JobException
     */
    public String appendValue(String path, String value) throws JobException
    {
        Object doc = _parser.load(value);
        Object newValue = appendNode(path, doc);

        if (newValue == null)
            return null;

        return _parser.dump(newValue);
    }

    @Override
    public String toString()
    {
        return dump();
    }


    // private methods


    /**
     * Parses a query to produce a set of segments
     *
     * @param path the query to parse
     * @return a list of objects that represents the query
     * @throws JobException
     */
    private List<QuerySegment> parsePath(String path) throws JobException
    {
        int offset = 0;
        int length = path.length();
        boolean collectingIndex = false;
        StringBuilder name = new StringBuilder(length);
        StringBuilder index = new StringBuilder(length);
        List<QuerySegment> result = new ArrayList<QuerySegment>();

        while (offset < length) {
            char current = path.charAt(offset);

            if (current == '.') {
                if (collectingIndex)
                    throw new JobException("unclosed bracket");

                if (!name.isEmpty()) {
                    result.add(new NodeName(name.toString()));

                    name = new StringBuilder(path.length() - offset);
                }

                if (!index.isEmpty()) {
                    result.add(new ArrayIndex(index.toString()));

                    index = new StringBuilder(path.length() - offset);
                }
            }
            else if (current == '[') {
                if (collectingIndex)
                    throw new JobException("unexpected opening bracket");

                collectingIndex = true;
            }
            else if (current == ']') {
                if (!collectingIndex)
                    throw new JobException("unexpected closing bracket");

                collectingIndex = false;
            }
            else if (current == '\\') {
                offset += 1;

                if (offset < length)
                    name.append(path.charAt(offset));
            }
            else {
                if (collectingIndex)
                    index.append(current);
                else {
                    if (!index.isEmpty())
                        throw new JobException("embedded brackets");

                    name.append(current);
                }
            }

            offset += 1;
        }

        if (!name.isEmpty())
            result.add(new NodeName(name.toString()));

        if (!index.isEmpty())
            result.add(new ArrayIndex(index.toString()));

        return result;
    }

    /**
     * Modifies a node or set of nodes selected by the given query
     *
     * @param path a query, assumed to not be null or empty
     * @param node a value to modify the selected nodes with
     * @param modifier a function that defines how the selected nodes will be
     * modified
     * @return the new values of the selected nodes
     * @throws JobException
     */
    private Object modifyNode(String path, Object node, NodeModifier modifier) throws JobException
    {
        List<QuerySegment> segmentList = parsePath(path);
        Iterator<QuerySegment> segments = segmentList.iterator();
        List<Object> nodes = new ArrayList<Object>();
        Object result;

        nodes.add(_document);

        while (true) {
            QuerySegment segment = segments.next();

            // if this is the last segment of the query, then the query has
            // succeeded, because the selector of this segment will either
            // match an existing node, or indicate how to create a new node
            // that satisfies the query
            if (!segments.hasNext()) {
                List<Object> results = modifier.modify(segment, nodes, node);

                if (results.isEmpty())
                    result = null;
                else if (results.size() == 1)
                    result = results.get(0);
                else
                    result = results;

                break;
            }

            nodes = segment.find(nodes);

            if (nodes.isEmpty()) {
                result = null;

                break;
            }
        }

        return result;
    }
}
