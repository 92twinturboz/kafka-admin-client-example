package com.dwittekind.ccloud;

import java.util.concurrent.ExecutionException;

public class RunAdminClient {
    public static void main(String[] args) {
        AdminClientExample example = new AdminClientExample();

        System.out.println("**** CONFIGURING CLIENT ****");
        example.setup();

        System.out.println("**** LISTING TOPIC NAMES ****");
        try {
            example.topicListing();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println("**** CREATE NEW TOPIC ****");
        example.createTestTopic();

        System.out.println("**** LISTING TOPIC NAMES AGAIN ****");
        try {
            example.topicListing();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println("**** UPDATE TOPIC RETENTION MS SETTING ****");
        try {
            example.updateTestTopic();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println("**** DELETING TOPIC ****");
        example.deleteTestTopic();

        System.out.println("**** LISTING TOPIC NAMES AGAIN ****");
        try {
            example.topicListing();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println("**** DESTROYING CLIENT ****");
        example.teardown();
    }
}
