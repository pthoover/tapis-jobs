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
 *
 * @author phoover
 */
public class YamlDocument
{
    // nested classes


    /**
     *
     */
    private static abstract class PathStep
    {
        /**
         *
         * @param nodes
         * @return
         */
        public abstract List<Object> find(List<Object> nodes);

        /**
         *
         * @param nodes
         * @param value
         * @return
         */
        public abstract List<Object> set(List<Object> nodes, Object value);

        /**
         *
         * @param nodes
         * @param value
         * @return
         */
        public abstract List<Object> append(List<Object> nodes, Object value);

        /**
         *
         * @param node
         * @return
         */
        @SuppressWarnings("unchecked")
        protected Object copy(Object node)
        {
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

    /*
     *
     */
    private static class NodeName
      extends PathStep
    {
        private final String _name;


        /*
         *
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

            if (((Map<?, ?>) node).containsKey(_name)) {
                Object target = ((Map<?, ?>) node).get(_name);

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

    /*
     *
     */
    private static class ArrayIndex
      extends PathStep
    {
        private final int _index;


        /*
         *
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
     *
     */
    @FunctionalInterface
    private interface NodeModifier
    {
        /**
         *
         * @param step
         * @param nodes
         * @param value
         * @return
         */
        List<Object> modify(PathStep step, List<Object> nodes, Object value);
    }


    // data fields


    private final Yaml _parser;
    private Object _document;


    // constructors


    /**
     *
     */
    public YamlDocument()
    {
        DumperOptions options = new DumperOptions();

        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

        _parser = new Yaml(options);
        _document = null;
    }


    // public methods


    /**
     *
     * @param input
     */
    public YamlDocument(InputStream input)
    {
        this();

        load(input);
    }

    /**
     *
     * @param input
     */
    public YamlDocument(Reader input)
    {
        this();

        load(input);
    }

    /**
     *
     * @param input
     */
    public YamlDocument(String input)
    {
        this();

        load(input);
    }

    /**
     *
     * @param input
     */
    public void load(InputStream input)
    {
        _document = _parser.load(input);
    }

    /**
     *
     * @param input
     */
    public void load(Reader input)
    {
        _document = _parser.load(input);
    }

    /**
     *
     * @param input
     */
    public void load(String input)
    {
        _document = _parser.load(input);
    }

    /**
     *
     * @return
     */
    public String dump()
    {
        if (_document == null)
            return null;

        return _parser.dump(_document);
    }

    /**
     *
     * @param path
     * @return
     * @throws JobException
     */
    public Object getNode(String path) throws JobException
    {
        if (path == null || path.isEmpty())
            return _document;

        List<PathStep> steps = parsePath(path);
        List<Object> nodes = new ArrayList<Object>();

        nodes.add(_document);

        for (PathStep step : steps) {
            nodes = step.find(nodes);

            if (nodes.isEmpty())
                return null;
        }

        if (nodes.size() == 1)
            return nodes.get(0);

        return nodes;
    }

    /**
     *
     * @param path
     * @param node
     * @throws JobException
     */
    public void setNode(String path, Object node) throws JobException
    {
        replaceNode(path, node);
    }

    /**
     *
     * @param path
     * @param node
     * @return
     * @throws JobException
     */
    public Object replaceNode(String path, Object node) throws JobException
    {
        if (path == null || path.isEmpty()) {
            Object previous = _document;

            _document = node;

            return previous;
        }

        return modifyNode(path, node, (PathStep step, List<Object> nodes, Object value) -> step.set(nodes, value));
    }

    /**
     *
     * @param path
     * @param node
     * @return
     * @throws JobException
     */
    @SuppressWarnings("unchecked")
    public Object appendNode(String path, Object node) throws JobException
    {
        if (path == null || path.isEmpty()) {
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

        return modifyNode(path, node, (PathStep step, List<Object> nodes, Object value) -> step.append(nodes, value));
    }

    /**
     *
     * @param path
     * @return
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
     *
     * @param path
     * @param value
     * @throws JobException
     */
    public void setValue(String path, String value) throws JobException
    {
        replaceValue(path, value);
    }

    /**
     *
     * @param path
     * @param value
     * @return
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
     *
     * @param path
     * @param value
     * @return
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

    /**
     *
     * @return
     */
    @Override
    public String toString()
    {
        return dump();
    }


    // private methods


    /*
     *
     */
    private List<PathStep> parsePath(String path) throws JobException
    {
        int offset = 0;
        int length = path.length();
        boolean collectingIndex = false;
        StringBuilder name = new StringBuilder(length);
        StringBuilder index = new StringBuilder(length);
        List<PathStep> result = new ArrayList<PathStep>();

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

    /*
     *
     */
    private Object modifyNode(String path, Object node, NodeModifier modifier) throws JobException
    {
        List<PathStep> stepList = parsePath(path);
        Iterator<PathStep> steps = stepList.iterator();
        List<Object> nodes = new ArrayList<Object>();
        Object result;

        nodes.add(_document);

        while (true) {
            PathStep step = steps.next();

            if (!steps.hasNext()) {
                List<Object> results = modifier.modify(step, nodes, node);

                if (results.isEmpty())
                    result = null;
                else if (results.size() == 1)
                    result = results.get(0);
                else
                    result = results;

                break;
            }

            nodes = step.find(nodes);

            if (nodes.isEmpty()) {
                result = null;

                break;
            }
        }

        return result;
    }
}
