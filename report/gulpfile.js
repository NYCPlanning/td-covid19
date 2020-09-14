'use strict';


const gulp = require('gulp')
const del = require('del')
const imagemin = require('gulp-imagemin')
const uglify = require('gulp-uglify')
const usemin = require('gulp-usemin')
const rev = require('gulp-rev')
const cleanCss = require('gulp-clean-css')
const flatmap = require('gulp-flatmap')
const htmlmin = require('gulp-htmlmin')
const zip = require('gulp-zip')
const daterev = require('gulp-date-rev')

gulp.task('archive', function () {
    return gulp.src('dist/**')
        .pipe(zip('dist.zip'))
        .pipe(daterev('dist.zip'))
        .pipe(gulp.dest('archive'));
});

gulp.task('clean', function () {
    return del(['dist']);
});

gulp.task('copyplotly', function () {
    return gulp.src('plotly/**')
        .pipe(gulp.dest('dist/plotly'));
});

gulp.task('copymapbox', function () {
    return gulp.src('mapbox/**')
        .pipe(gulp.dest('dist/mapbox'));
});

gulp.task('copypdf', function () {
    return gulp.src('pdf/**')
        .pipe(gulp.dest('dist/pdf'));
});

gulp.task('imagemin', function () {
    return gulp.src('img/**/*.{png,jpg,gif}')
        .pipe(imagemin({ optimizationLevel: 3, progressive: true, interlaced: true }))
        .pipe(gulp.dest('dist/img'));
});

gulp.task('usemin', function () {
    return gulp.src('index.html')
        .pipe(flatmap(function (stream, file) {
            return stream
                .pipe(usemin({
                    html: [function () { return htmlmin({ collapseWhitespace: true }) }],
                    css: [rev()],
                    js: [uglify(), rev()],
                    inlinejs: [uglify()],
                    inlinecss: [cleanCss(), 'concat']
                }))
        }))
        .pipe(gulp.dest('dist/'));
});

gulp.task('build', gulp.series('archive', 'clean', gulp.parallel('copyplotly', 'copymapbox', 'copypdf', 'imagemin', 'usemin')), function (done) {
    done();
});

