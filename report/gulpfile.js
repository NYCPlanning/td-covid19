'use strict';


var gulp = require('gulp'),
    del = require('del'),
    imagemin=require('gulp-imagemin'),
    uglify =require('gulp-uglify'),
    usemin=require('gulp-usemin'),
    rev=require('gulp-rev'),
    cleanCss=require('gulp-clean-css'),
    flatmap=require('gulp-flatmap'),
    htmlmin=require('gulp-htmlmin');

gulp.task('clean',function(){
    return del(['dist']);
});

gulp.task('imagemin',function(){
    return gulp.src('img/*.{png,jpg,gif}')
    .pipe(imagemin({optimizationLevel:3, progressive:true,interlaced:true}))
    .pipe(gulp.dest('dist/img'));
});

gulp.task('usemin',function(){
    return gulp.src('./*.html')
    .pipe(flatmap(function(stream,file){
        return stream
        .pipe(usemin({
            css:[rev()],
            html:[function(){return htmlmin({collapseWhitespace:true})}],
            js:[uglify(),rev()],
            inlinejs:[uglify()],
            inlinecss:[cleanCss(),'concat']
        }))
    }))
    .pipe(gulp.dest('dist/'));
});

gulp.task('build',gulp.series('clean',gulp.parallel('imagemin','usemin')),function(done){
    done();
});