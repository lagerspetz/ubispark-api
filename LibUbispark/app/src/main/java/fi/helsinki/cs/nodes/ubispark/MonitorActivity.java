package fi.helsinki.cs.nodes.ubispark;

import android.os.Bundle;
import android.support.design.widget.FloatingActionButton;
import android.support.design.widget.Snackbar;
import android.view.View;
import android.support.design.widget.NavigationView;
import android.support.v4.view.GravityCompat;
import android.support.v4.widget.DrawerLayout;
import android.support.v7.app.ActionBarDrawerToggle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.view.Menu;
import android.view.MenuItem;
import android.widget.TextView;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import fi.helsinki.cs.nodes.libubispark.LocalRunner;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.collection.immutable.Range;
import scala.collection.immutable.Seq;

import java.util.concurrent.ForkJoinTask;

public class MonitorActivity extends AppCompatActivity
        implements NavigationView.OnNavigationItemSelectedListener {

    private LocalRunner runner;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_monitor);
        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);

        FloatingActionButton fab = (FloatingActionButton) findViewById(R.id.fab);
        fab.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Snackbar.make(view, "Replace with your own action", Snackbar.LENGTH_LONG)
                        .setAction("Action", null).show();
            }
        });

        DrawerLayout drawer = (DrawerLayout) findViewById(R.id.drawer_layout);
        ActionBarDrawerToggle toggle = new ActionBarDrawerToggle(
                this, drawer, toolbar, R.string.navigation_drawer_open, R.string.navigation_drawer_close);
        drawer.setDrawerListener(toggle);
        toggle.syncState();

        NavigationView navigationView = (NavigationView) findViewById(R.id.nav_view);
        navigationView.setNavigationItemSelectedListener(this);
    }

    @Override
    public void onBackPressed() {
        DrawerLayout drawer = (DrawerLayout) findViewById(R.id.drawer_layout);
        if (drawer.isDrawerOpen(GravityCompat.START)) {
            drawer.closeDrawer(GravityCompat.START);
        } else {
            super.onBackPressed();
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.monitor, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {
            return true;
        }

        return super.onOptionsItemSelected(item);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public boolean onNavigationItemSelected(MenuItem item) {
        // Handle navigation view item clicks here.
        int id = item.getItemId();

        if (id == R.id.nav_camera) {
            // Handle the camera action
            localRunnerTest();
        } else if (id == R.id.nav_gallery) {

        } else if (id == R.id.nav_slideshow) {

        } else if (id == R.id.nav_manage) {

        } else if (id == R.id.nav_share) {

        } else if (id == R.id.nav_send) {

        }

        DrawerLayout drawer = (DrawerLayout) findViewById(R.id.drawer_layout);
        drawer.closeDrawer(GravityCompat.START);
        return true;
    }

    private void scTest() {
      /*  List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10000; i++){
            list.add(i);
        }
        JavaRDD<Integer> paral = sc.parallelize(list, 100);

        final Integer output = paral.reduce(new org.apache.spark.api.java.function.Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        runOnUiThread(new Runnable() {
            @Override
            public void run() {
                final TextView tv = ((TextView) findViewById(R.id.textView));
                tv.setText(tv.getText() + " " + output + "");
            }
        });*/
    }

    private void localRunnerTest() {
        new Thread() {
            public void run() {
                if (runner == null)
                    runner = new LocalRunner(4);
                int max = 10000;
                List<Callable<Double>> items = new LinkedList<>();
                for (int i = 0; i < max; i++)
                    items.add(new Callable<Double>() {
                        @Override
                        public Double call() throws Exception {
                            return Math.random();
                        }
                    });
                final List<ForkJoinTask<Double>> outputs = new LinkedList<>();
                for (Callable<Double> it : items) {
                    outputs.add(runner.scheduleTask(it));
                }

                new Thread(){
                    public void run() {
                        for (ForkJoinTask<Double> task : outputs) {
                            final double res = task.join();
                            runOnUiThread(new Runnable() {
                                @Override
                                public void run() {
                                    final TextView tv = ((TextView) findViewById(R.id.textView));
                                    tv.setText(tv.getText() + " " + res + "");
                                }
                            });
                        }
                    }
                }.start();

            }
        }.start();
    }

    public double squareSum() {
        return Math.pow(Math.random(), 2);
    }
}
