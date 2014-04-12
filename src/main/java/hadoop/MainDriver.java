/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hadoop;

import hadoop.movie.CountMoviesPerActorSortDescending;
import hadoop.movie.MovieCount;
import hadoop.movie.ShowActorsInMovie;

import org.apache.hadoop.util.ProgramDriver;


/**
 * A description of an example program based on its class and a 
 * human-readable description.
 */
public class MainDriver {
  
  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
   
      pgd.addClass("moviecount", MovieCount.class, 
                "A map/reduce program that counts # of movies for each year");
     
      pgd.addClass("ShowActorsInMovie", ShowActorsInMovie.class, 
              "for each movie, show who are the actors");
      
      pgd.addClass("CountMoviesPerActorSortDescending", CountMoviesPerActorSortDescending.class, 
              "for each actor, show how many movies he/she has played in");
      
      pgd.driver(argv);
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
	
