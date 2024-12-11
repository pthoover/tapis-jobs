package edu.utexas.tacc.tapis.jobs.utils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.jobs.exceptions.JobException;


@Test(groups={"unit"})
public class YamlDocTest {

    @Test
    public void getValueTest() throws JobException
    {
        YamlDocument doc = new YamlDocument("{one: 1}");

        Assert.assertEquals(doc.getValue(null), "one: 1\n");
        Assert.assertEquals(doc.getValue("one"), "1\n");

        doc.load("{one: {two: 2}}");

        Assert.assertEquals(doc.getValue("one.two"), "2\n");

        doc.load("{one: {two: [2, 3]}}");

        Assert.assertEquals(doc.getValue("one.two"), "- 2\n- 3\n");
        Assert.assertEquals(doc.getValue("one.two[1]"), "3\n");

        doc.load("{one: [{two: 2}]}");

        Assert.assertEquals(doc.getValue("one.two"), "2\n");

        doc.load("{one: {two: {three: 3, four: 4}}}");

        Assert.assertEquals(doc.getValue("one.two"), "three: 3\nfour: 4\n");

        doc.load("{one: {two: {three.four: 3.4}}}");

        Assert.assertEquals(doc.getValue("one.two.three\\.four"), "3.4\n");

        doc.load("{one: 1, two: 2}");

        Assert.assertNull(doc.getValue("foo"));
        Assert.assertNull(doc.getValue("two.bar"));
    }

    @Test
    public void setNodeTest() throws JobException
    {
        YamlDocument doc = new YamlDocument("{one: 1}");

        doc.setNode("one", 2);

        Assert.assertEquals(doc.dump(), "one: 2\n");

        List<Integer> listNode = new ArrayList<Integer>(2);

        listNode.add(2);
        listNode.add(3);
        doc.load("{one: {two: 2}}");
        doc.setNode("one.two", listNode);

        Assert.assertEquals(doc.dump(), "one:\n  two:\n  - 2\n  - 3\n");

        doc.setNode("one.two[1]", 4);

        Assert.assertEquals(doc.dump(), "one:\n  two:\n  - 2\n  - 4\n");

        Map<String, Integer> mapNode = new LinkedHashMap<String, Integer>(2);

        mapNode.put("three", 3);
        mapNode.put("four", 4);
        doc.load("{one: {two: 2}}");
        doc.setNode("one.two", mapNode);

        Assert.assertEquals(doc.dump(), "one:\n  two:\n    three: 3\n    four: 4\n");
    }

    @Test
    public void setValueTest() throws JobException
    {
        YamlDocument doc = new YamlDocument("{one: 1}");

        doc.setValue("one", "2");

        Assert.assertEquals(doc.dump(), "one: 2\n");

        doc.load("{one: {two: 2}}");
        doc.setValue("one.two", "3");

        Assert.assertEquals(doc.dump(), "one:\n  two: 3\n");

        doc.load("{one: {two: [2, 3]}}");
        doc.setValue("one.two[1]", "4");

        Assert.assertEquals(doc.dump(), "one:\n  two:\n  - 2\n  - 4\n");

        doc.load("{one: {two: 2}}");
        doc.setValue("one.two", "{three: 3, four: 4}");

        Assert.assertEquals(doc.dump(), "one:\n  two:\n    three: 3\n    four: 4\n");

        doc.load("{one: {two: 2}}");
        doc.setValue("one.three", "3");

        Assert.assertEquals(doc.dump(), "one:\n  two: 2\n  three: 3\n");

        doc.load("{one: [{two: 2}, {two: 2}]}");
        doc.setValue("one.two", "{three: 3}");

        Assert.assertEquals(doc.dump(), "one:\n- two:\n    three: 3\n- two:\n    three: 3\n");
    }

    @Test
    public void replaceNodeTest() throws JobException
    {
        YamlDocument doc = new YamlDocument("{one: 1}");
        Object value = doc.replaceNode("one", 2);

        Assert.assertEquals(value.toString(), "1");
        Assert.assertEquals(doc.dump(), "one: 2\n");
    }

    @Test
    public void replaceValueTest() throws JobException
    {
        YamlDocument doc = new YamlDocument("{one: 1}");
        String value = doc.replaceValue("one", "2");

        Assert.assertEquals(value, "1\n");
        Assert.assertEquals(doc.dump(), "one: 2\n");
    }

    @Test
    public void appendNodeTest() throws JobException
    {
        YamlDocument doc = new YamlDocument("{one: 1}");

        doc.appendNode("one", 2);

        Assert.assertEquals(doc.dump(), "one:\n- 1\n- 2\n");

        Map<String, Integer> mapNode = new LinkedHashMap<String, Integer>(2);

        mapNode.put("three", 3);
        mapNode.put("four", 4);
        doc.load("{one: {two: 2}}");
        doc.appendNode("one.two", mapNode);

        Assert.assertEquals(doc.dump(), "one:\n  two:\n  - 2\n  - three: 3\n    four: 4\n");
    }

    @Test
    public void appendValueTest() throws JobException
    {
        YamlDocument doc = new YamlDocument("{one: 1}");
        String value = doc.appendValue("one", "2");

        Assert.assertEquals(value, "- 1\n- 2\n");
        Assert.assertEquals(doc.dump(), "one:\n- 1\n- 2\n");

        doc.load("{one: {two: [2, 3]}}");

        value = doc.appendValue("one.two", "4");

        Assert.assertEquals(value, "- 2\n- 3\n- 4\n");
        Assert.assertEquals(doc.dump(), "one:\n  two:\n  - 2\n  - 3\n  - 4\n");
    }
}
