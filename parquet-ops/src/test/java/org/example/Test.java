package org.example;

import com.rits.cloning.Cloner;
import org.apache.commons.beanutils.BeanUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class Test {

    @org.junit.Test
    public void test() throws Exception {
     /*   String[] a = "dfasd|dsfasd|dfasdaf|dfadsf|dfasdf|dfasdf|dfasdf|".split("\\|", -1);

        for (int i = 0; i < 10; i++) {
            String ss = a[i];
        }*/

        Task task = new Task();
        task.setName("task");

        Task.SubTask subTask = new Task.SubTask();
        subTask.setId("0001");

        task.setSubTask(subTask);

        System.out.println("01 = " + task);

        Task newTask = clone(task);
        System.out.println(newTask);

        Properties prop = new Properties();
        prop.load(new FileInputStream("resource/mysql.properties"));

    }

    public static <T> T clone(final T object) {
        if (object == null) {
            return null;
        }
        return new Cloner().deepClone(object);
    }

    public static class Task {
        private String name;
        private SubTask subTask;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public SubTask getSubTask() {
            return subTask;
        }

        public void setSubTask(SubTask subTask) {
            this.subTask = subTask;
        }

        public static class SubTask {
            private String id;

            public String getId() {
                return id;
            }

            public void setId(String id) {
                this.id = id;
            }

            @Override
            public String toString() {
                return "SubTask{" +
                        "id='" + id + '\'' +
                        '}';
            }
        }

        @Override
        public String toString() {
            return "Task{" +
                    "name='" + name + '\'' +
                    ", subTask=" + subTask +
                    '}';
        }
    }


}
